/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const stream = require('stream');
const assert = require('assert');
const dbg = require('../util/debug_module')(__filename);

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

            // caching.ttl is in milliseconds
            if (time_since_validation <= this.caching.ttl) {
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

    async read_object_stream(params, object_sdk) {

        const get_from_cache = params.get_from_cache;
        if (get_from_cache) {
            // Remove get_from_cache if exists for matching RPC schema in later API calls
            params = _.omit(params, 'get_from_cache');
        }

        if (params.object_md.should_read_from_cache || get_from_cache) {
            try {
                // params.missing_range_handler = async () => {
                //     this._read_from_hub(params, object_sdk);
                // };

                dbg.log0('NamespaceCache.read_object_stream: read from cache', params);
                return this.namespace_nb.read_object_stream(params, object_sdk);
            } catch (err) {
                dbg.warn('NamespaceCache.read_object_stream: cache error', err);
            }
        }

        return this._read_from_hub(params, object_sdk);
    }

    async _read_from_hub(params, object_sdk) {
        const read_stream = await this.namespace_hub.read_object_stream(params, object_sdk);

        // we use a pass through stream here because we have to start piping immediately
        // and the cache upload does not pipe immediately (only after creating the object_md).
        const cache_stream = new stream.PassThrough();
        read_stream.pipe(cache_stream);

        const upload_params = {
            source_stream: cache_stream,
            bucket: params.bucket,
            key: params.key,
            size: params.object_md.size,
            content_type: params.content_type,
            xattr: params.object_md.xattr,
        };

        dbg.log0('NamespaceCache.read_object_stream: put to cache',
            _.omit(upload_params, 'source_stream'));


        // PUT MISSING RANGES HERE ----

        this.namespace_nb.upload_object(upload_params, object_sdk);

        return read_stream;
    }

    ///////////////////
    // OBJECT UPLOAD //
    ///////////////////

    async upload_object(params, object_sdk) {
        dbg.log0("NamespaceCache.upload_object", _.omit(params, 'source_stream'));

        if (params.size > 1024 * 1024) {

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
