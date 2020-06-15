/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const stream = require('stream');
const assert = require('assert');
const dbg = require('../util/debug_module')(__filename);
const config = require('../../config.js');
//const js_utils = require('../util/js_utils');
const range_utils = require('../util/range_utils');
const RangeStream = require('../util/range_stream');

class NamespaceCache {

    constructor({ namespace_hub, namespace_nb, caching, active_triggers }) {
        this.namespace_hub = namespace_hub;
        this.namespace_nb = namespace_nb;
        this.active_triggers = active_triggers;
        this.caching = caching;
    }

    get_write_resource() {
        return this.namespace_hub;
    }

    _delete_object_from_cache(params, object_sdk) {
        process.nextTick(() => {
            const delete_params = _.pick(params, 'bucket', 'key');
            dbg.log0("NamespaceCache._delete_object_from_cache", delete_params);
            this.namespace_nb.delete_object(delete_params, object_sdk)
            .then(() => {
                dbg.log0('NamespaceCache: deleted object from cache', delete_params);
            })
            .catch(err_delete => {
                dbg.warn('NamespaceCache: error in deleting object from cache', params, err_delete);
            });
        });
    }

    // Return block info (start and end block index) for range in read
    // Return undefined if non range read
    _get_block_idx(params) {
        const { start, end } = params;
        if (start === undefined) return;

        const block_size = config.NAMESPACE_CACHING.DEFAULT_BLOCK_SIZE;
        const start_block_idx = start / block_size;
        const end_end_idx = end / block_size;
        return { start_block_idx, end_end_idx };
    }

    // Determine whether range read should be performed on hub
    // Return block info that sets the range in reads from hub
    // Return undefined otherwise
    _range_read_hub_check(params) {
        const block_info = this._get_block_idx(params);
        if (block_info) {
            let block_size = config.NAMESPACE_CACHING.MAX_CACHE_OBJECT_SIZE;
            if (params.object_md.size <= block_size) return;
            if (params.object_md.size <= config.NAMESPACE_CACHING.MAX_CACHE_OBJECT_SIZE) {
                const range_size = (block_info.start_block_idx - block_info.start_block_idx + 1) * block_size;
                if (range_size > params.object_md.size * 4 / 5) return;
            }
        }
        return block_info;
    }

    // async _find_missing_parts_cache(params, object_sdk, parts) {
    //     const list_params = _.pick(params, 'bucket', 'key', 'obj_id');
    //     list_params.obj_id = params.object_md.obj_id;
    //     // TODO: implement LRU cache
    //     const list_reply = await object_sdk.rpc_client.object.list_object_parts(list_params);
    //     const num_cached_parts = list_reply.part.length;

    //     const min_cached_block_idx = list_reply.parts[0].seq;
    //     const max_cached_block_idx = list_reply.parts[num_cached_parts - 1].seq;
    //     if (num_cached_parts === 0) return parts;
    //     if (parts.end_block_idx < min_cached_block_idx || parts.start_block_idx > max_cached_block_idx) return parts;

    //     const block_idx_list = _.range(parts.start_block_idx, parts.end_block_idx + 1);
    //     const missing_blocks = block_idx_list.filter(idx => {
    //         if (idx < min_cached_block_idx || idx > max_cached_block_idx) return true;
    //         const found_idx = js_utils.findSortedIndexBy(list_reply.parts, { seq: idx }, p => p.seq);
    //         if (found_idx < 0) return true;
    //         return false;
    //     });

    //     return missing_blocks;
    // }

    /////////////////
    // OBJECT LIST //
    /////////////////

    async list_objects(params, object_sdk) {
        // TODO listing from cache only for deevelopment
        return this.namespace_nb.list_objects(params, object_sdk);
    }

    async list_uploads(params, object_sdk) {
        // TODO listing from cache only for deevelopment
        return this.namespace_nb.list_uploads(params, object_sdk);
    }

    async list_object_versions(params, object_sdk) {
        // TODO listing from cache only for deevelopment
        return this.namespace_nb.list_object_versions(params, object_sdk);
    }

    /////////////////
    // OBJECT READ //
    /////////////////

    async read_object_md(params, object_sdk) {
        let object_info_cache = null;
        let cache_etag = '';
        try {
            const get_from_cache = params.get_from_cache;
            if (get_from_cache) {
                // Remove get_from_cache if exists for maching RPC schema
                params = _.omit(params, 'get_from_cache');
            }
            object_info_cache = await this.namespace_nb.read_object_md(params, object_sdk);
            if (get_from_cache) {
                dbg.log0('NamespaceCache.read_object_md get_from_cache is enabled', object_info_cache);
                object_info_cache.should_read_from_cache = true;
                return object_info_cache;
            }

            const cache_validation_time = object_info_cache.cache_valid_time;
            const time_since_validation = Date.now() - cache_validation_time;

            // caching.ttl is in seconds
            if (time_since_validation <= this.caching.ttl * 1000) {
                object_info_cache.should_read_from_cache = true; // mark it for read_object_stream
                dbg.log0('NamespaceCache.read_object_md use md from cache', object_info_cache);
                return object_info_cache;
            }

            cache_etag = object_info_cache.etag;
        } catch (err) {
            dbg.log0('NamespaceCache.read_object_md: error in cache', err);
        }

        let object_info_hub = null;
        try {
            object_info_hub = await this.namespace_hub.read_object_md(params, object_sdk);
            if (object_info_hub.etag === cache_etag) {
                dbg.log0('NamespaceCache.read_object_md: same etags: updating cache valid time', object_info_hub);
                process.nextTick(() => {
                    const update_params = _.pick(_.defaults({ bucket: this.namespace_nb.target_bucket }, params), 'bucket', 'key');
                    update_params.cache_valid_time = (new Date()).getTime();
                    object_sdk.rpc_client.object.update_object_md(update_params)
                        .then(() => {
                            dbg.log0('NamespaceCache.read_object_md: updated cache valid time', update_params);
                        })
                        .catch(err => {
                            dbg.error('NamespaceCache.read_object_md: error in updating cache valid time', err);
                        });
                });

                object_info_cache.should_read_from_cache = true;
                return object_info_cache;

            } else if (cache_etag === '') {
                object_info_hub.should_read_from_cache = false;
            } else {
                dbg.log0('NamespaceCache.read_object_md: etags different',
                    params, {hub_tag: object_info_hub.etag, cache_etag: cache_etag});
            }
        } catch (err) {
            if (err.code === 'NoSuchKey') {
                if (object_info_cache) {
                    this._delete_object_from_cache(params, object_sdk);
                }
            } else {
                dbg.error('NamespaceCache.read_object_md: NOT NoSuchKey in hub', err);
            }
            throw (err);
        }
        return object_info_hub;
    }

    // It performs range reads on hub
    async _range_read_hub_object_stream(params, object_sdk) {
        dbg.log0('NamespaceCache._range_read_hub_object_stream', {params: params});

        const block_size = config.NAMESPACE_CACHING.DEFAULT_BLOCK_SIZE;
        if (!params.object_md.should_read_from_cache) {
            const create_params = _.pick(params,
                'bucket',
                'key',
                'content_type',
                'size',
                'etag',
                'xattr',
                'partial_object'
            );
            create_params.size = params.object_md.size;
            create_params.etag = params.object_md.etag;
            create_params.xattr = params.object_md.xattr;
            create_params.partial_object = true;
            const create_reply = await object_sdk.rpc_client.object.create_object_upload(create_params);
            dbg.log0('NamespaceCache._range_read_hub_object_stream: partial upload created:', create_reply.obj_id);

            const aligned_read_start = range_utils.align_down(params.start, block_size);
            const aligned_read_end = range_utils.align_up(params.end, block_size);
            params.object_md.obj_id = create_reply.obj_id;

            // Read aligned range of bytes
            return this._read_hub_object_stream(params, object_sdk, { start: aligned_read_start, end: aligned_read_end });
        }

        const read_params = _.omit(params, ['start', 'end']);
        let self = this;
        params.missing_part_getter = async function(missing_part_start, missing_part_end) {
            dbg.log0('NamespaceCache._range_read_hub_object_stream: missing_part_getter',
                {params: params, missing_part_start, missing_part_end});

            const hub_read_start = range_utils.align_down(missing_part_start, block_size);
            const hub_read_end = range_utils.align_up(missing_part_end, block_size);

            read_params.start = missing_part_start;
            read_params.end = missing_part_end;
            return new Promise((resolve, reject) => {
                self._read_hub_object_stream(read_params, object_sdk, { start: hub_read_start, end: hub_read_end })
                .then(read_stream => {
                    const bufs = [];
                    read_stream.on('data', data => bufs.push(data));
                    read_stream.on('end', () => {
                        const ret = Buffer.concat(bufs);
                        resolve(ret);
                    });
                    read_stream.on('error', err => {
                        dbg.error('NamespaceCache.missing_part_getter: stream error', err);
                        throw err;
                    });
                })
                .catch(err => {
                    dbg.error('NamespaceCache.missing_part_getter: _read_hub_object_stream error', err);
                    throw err;
                });
            });
        };
        return this.namespace_nb.read_object_stream(params, object_sdk);
    }

    /*
     * If hub_read_range is provided, it performs range read from hub.
     * Otherwise, perform entire object read from hub.
     *
     *                     |-- pipe if read size is <= configured max cached object size --> cache_upload_stream
     * hub_read_stream --> |
     *                     |-- pipe if range read --> range_stream
     *
     * Returns range_stream if range read; otherwise, hub_read_stream
     *
     */
    async _read_hub_object_stream(params, object_sdk, hub_read_range) {
        dbg.log0('NamespaceCache._read_hub_object_stream', {params: params, hub_read_range});

        let read_size = params.object_md.size;
        const hub_read_params = _.omit(params, ['start', 'end']);

        if (hub_read_range) {
            hub_read_range.end = Math.min(params.object_md.size, hub_read_range.end);
            hub_read_params.start = hub_read_range.start;
            // end for NamespaceS3 is exclusive
            hub_read_params.end = hub_read_range.end;

            read_size = hub_read_range.end - hub_read_range.start;
        }

        const hub_read_stream = await this.namespace_hub.read_object_stream(hub_read_params, object_sdk);

        let range_stream;
        if (params.start || params.end) {
            let start = params.start;
            let end = params.end;
            if (hub_read_range) {
                start -= hub_read_range.start;
                end -= hub_read_range.start;
            }
            range_stream = new RangeStream(start, end);
            hub_read_stream.pipe(range_stream);
        }

        // Object or part will only be uploaded to cache if size is not too big
        if (read_size <= config.NAMESPACE_CACHING.MAX_CACHE_OBJECT_SIZE) {
            // we use a pass through stream here because we have to start piping immediately
            // and the cache upload does not pipe immediately (only after creating the object_md).
            const cache_upload_stream = new stream.PassThrough();
            hub_read_stream.pipe(cache_upload_stream);

            const upload_params = {
                source_stream: cache_upload_stream,
                bucket: params.bucket,
                key: params.key,
                size: params.object_md.size,
                content_type: params.content_type,
                xattr: params.object_md.xattr,
            };
            if (hub_read_range) {
                upload_params.start = hub_read_range.start;
                upload_params.end = hub_read_range.end;
                // Set object ID since partial object has been created before
                upload_params.obj_id = params.object_md.obj_id;

                this.namespace_nb.upload_range_part(upload_params, object_sdk);

                dbg.log0('NamespaceCache._read_hub_object_stream: started uploading part to cache');
            } else {
                this.namespace_nb.upload_object(upload_params, object_sdk);
                dbg.log0('NamespaceCache._read_hub_object_stream: object uploaded to cache');
            }
        }

        const ret_stream = range_stream ? range_stream : hub_read_stream;
        return ret_stream;
    }


    async read_object_stream(params, object_sdk) {
        const get_from_cache = params.get_from_cache;
        if (get_from_cache) {
            // Remove get_from_cache if exists for matching RPC schema
            params = _.omit(params, 'get_from_cache');
        }

        if ((params.object_md.should_read_from_cache && !params.object_md.partial_object) || get_from_cache) {
            // Cache should have entire object
            try {
                dbg.log0('NamespaceCache.read_object_stream: read from cache', {params: params});
                return this.namespace_nb.read_object_stream(params, object_sdk);
            } catch (err) {
                dbg.warn('NamespaceCache.read_object_stream: cache error', err);
            }
        }

        const range_read = this._range_read_hub_check(params);
        if (range_read) {
            return this._range_read_hub_object_stream(params, object_sdk);
        }

        // Hub read is NOT range read
        return this._read_hub_object_stream(params, object_sdk);
    }

    ///////////////////
    // OBJECT UPLOAD //
    ///////////////////

    async upload_object(params, object_sdk) {

        dbg.log0("NamespaceCache.upload_object", _.omit(params, 'source_stream'));

        if (params.size > config.NAMESPACE_CACHING.MAX_CACHE_OBJECT_SIZE) {

            this._delete_object_from_cache(params, object_sdk);

            return this.namespace_hub.upload_object(params, object_sdk);

        } else {

            // UPLOAD SIMULTANEOUSLY TO BOTH

            const hub_stream = new stream.PassThrough();
            const hub_params = { ...params, source_stream: hub_stream };
            const hub_promise = this.namespace_hub.upload_object(hub_params, object_sdk);

            // defer the final callback of the cache stream until the hub ack
            const cache_finalizer = callback => hub_promise.then(() => callback(), err => callback(err));
            const cache_stream = new stream.PassThrough({ final: cache_finalizer });
            const cache_params = { ...params, source_stream: cache_stream };
            const cache_promise = this.namespace_nb.upload_object(cache_params, object_sdk);

            // One important caveat is that if the Readable stream emits an error during processing,
            // the Writable destination is not closed automatically. If an error occurs, it will be
            // necessary to manually close each stream in order to prevent memory leaks.
            params.source_stream.on('error', err => {
                dbg.log0("NamespaceCache.upload_object: error in read source", {params: _.omit(params, 'source_stream'), error: err});
                hub_stream.destroy();
                cache_stream.destroy();
            });

            params.source_stream.pipe(hub_stream);
            params.source_stream.pipe(cache_stream);

            const hub_upload = await Promise.all([ hub_promise, cache_promise ])
            .then(([hub_res, cache_res]) => {
                assert.strictEqual(hub_res.etag, cache_res.etag);
                return hub_res;
            })
            .catch(err => {
                dbg.log0("NamespaceCache.upload_object: error in upload", {params, error: err});
                // If error is from cache, we should probably ignore and let hub upload continue. Change Promise.all to Promise.allSettled?
                throw err;
            });

            return hub_upload;
        }
    }

    //////////////////////
    // MULTIPART UPLOAD //
    //////////////////////

    async create_object_upload(params, object_sdk) {
        return this.namespace_hub.create_object_upload(params, object_sdk);
    }

    async upload_multipart(params, object_sdk) {
        return this.namespace_hub.upload_multipart(params, object_sdk);
    }

    async list_multiparts(params, object_sdk) {
        return this.namespace_hub.list_multiparts(params, object_sdk);
    }

    async complete_object_upload(params, object_sdk) {

        // TODO: INVALIDATE CACHE
        // await this.namespace_nb.delete_object(TODO);

        return this.namespace_hub.complete_object_upload(params, object_sdk);
    }

    async abort_object_upload(params, object_sdk) {
        return this.namespace_hub.abort_object_upload(params, object_sdk);
    }

    ///////////////////
    // OBJECT DELETE //
    ///////////////////

    async delete_object(params, object_sdk) {

        /*
            // DELETE CACHE
            try {
                await this.namespace_nb.delete_object(params, object_sdk);
            } catch (err) {
                if (err !== 'NotFound') throw;
            }

            // DELETE HUB
            return this.namespace_hub.delete_object(params, object_sdk);
      */

        const [hub_res, cache_res] = await Promise.allSettled([
            this.namespace_hub.delete_object(params, object_sdk),
            this.namespace_nb.delete_object(params, object_sdk),
        ]);
        if (hub_res.status === 'rejected') {
            throw hub_res.reason;
        }
        if (cache_res.status === 'rejected' &&
            cache_res.reason.code !== 'NoSuchKey') {
            throw cache_res.reason;
        }
        return hub_res.value;
    }

    async delete_multiple_objects(params, object_sdk) {
        return this.namespace_hub.delete_multiple_objects(params, object_sdk);
    }

    ////////////////////
    // OBJECT TAGGING //
    ////////////////////

    async get_object_tagging(params, object_sdk) {
        return this.namespace_hub.get_object_tagging(params, object_sdk);
    }

    async delete_object_tagging(params, object_sdk) {
        return this.namespace_hub.delete_object_tagging(params, object_sdk);
    }

    async put_object_tagging(params, object_sdk) {
        return this.namespace_hub.put_object_tagging(params, object_sdk);
    }

    //////////////////////////
    // AZURE BLOB MULTIPART //
    //////////////////////////

    async upload_blob_block(params, object_sdk) {
        return this.namespace_hub.upload_blob_block(params, object_sdk);
    }

    async commit_blob_block_list(params, object_sdk) {
        return this.namespace_hub.commit_blob_block_list(params, object_sdk);
    }

    async get_blob_block_lists(params, object_sdk) {
        return this.namespace_hub.get_blob_block_lists(params, object_sdk);
    }

}


module.exports = NamespaceCache;
