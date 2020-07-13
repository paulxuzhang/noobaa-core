/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const P = require('../../util/promise');
const promise_utils = require('../../util/promise_utils');

const test_scenarios = [
    'range read: initial read size is < block_size and not across block boundary',
];

async function test_case_range_read_initial_read_size_not_across_blocks({ type, ns_context }) {

    // Make file big enough for holding multiple blocks
    const prefix = `file_${(Math.floor(Date.now() / 1000))}_${type}`;
    const { block_size, block_size_kb } = ns_context;
    const file_name = `${prefix}_${block_size_kb * 5}_KB`;
    const delay_ms = ns_context.cache_ttl_ms + 1000;
    let time_start = (new Date()).getTime();

    // Expect block3 will be cached
    // blocks     :  |       b0        |       b1         |       b2      |  b3(to be cached) |   .....
    // read range :                                                         <-->
    let range_size = 100;
    let start = (block_size * 3) + 100;
    let end = start + range_size - 1;

    await ns_context.upload_directly_to_cloud({ type, file_name });
    console.log(`Reading range ${start}-${end} in ${file_name}`);
    await ns_context.validate_md5_range_read_between_hub_and_cache({
        type,
        file_name,
        start,
        end,
        expect_read_size: range_size,
        expect_same: true
    });
    const cloud_obj_md = ns_context.get_object_s3_md_via_cloud(type, file_name);
    let time_end = (new Date()).getTime();
    await promise_utils.wait_until(async () => {
        try {
            await ns_context.valid_cache_object_noobaa_md({
                type,
                file_name,
                validation_params: {
                    cache_last_valid_time_range: {
                        start: time_start,
                        end: time_end
                    },
                    size: cloud_obj_md.size,
                    etag: cloud_obj_md.etag,
                    partial_object: true,
                    num_parts: 1,
                    upload_size: block_size
                }
            });
            return true;
        } catch (err) {
            if (err.rpc_code === 'NO_SUCH_OBJECT') return false;
            throw err;
        }
    }, 10000);

    // Expect block1 will be cached, so we will have block1 and block3 cached
    // blocks     :  |       b0        | b1(to be cached) |       b2      |    b3(cached)     |   .....
    // read range :                      <-->
    range_size = 200;
    start = block_size + 100;
    end = start + range_size - 1;
    // Read the same range twice.
    for (let i = 0; i < 2; i++) {
        await ns_context.validate_range_read({
            type, file_name, cloud_obj_md,
            start, end,
            expect_read_size: range_size,
            upload_size: block_size * 2,
            expect_num_parts: 2,
            cache_last_valid_time_range: {
                start: time_start,
                end: time_end
            }
        });
    }

    // Expect block0 and block2 to be cached
    // blocks     :  | b0(to be cached)|    b1(cached)    | b2(to be cached) |    b3(cached)     |   .....
    // read range :    <------------------------------------>
    range_size = block_size * 2;
    start = 100;
    end = start + range_size - 1;
    await ns_context.validate_range_read({
        type, file_name, cloud_obj_md,
        start, end,
        expect_read_size: range_size,
        expect_num_parts: 4,
        expect_upload_size: block_size * 4,
        cache_last_valid_time_range: {
            start: time_start,
            end: time_end
        }
    });

    // Expect range read to come from cache
    // blocks     :  |    b0(cached)    |    b1(cached)    |    b2(cached)    |    b3(cached)     |   .....
    // read range :    <-------------------------------------------------------->
    range_size = block_size * 3;
    start = 100;
    end = start + range_size - 1;
    await ns_context.validate_range_read({
        type, file_name, cloud_obj_md,
        start, end,
        expect_read_size: range_size,
        expect_num_parts: 4,
        expect_upload_size: block_size * 4,
        cache_last_valid_time_range: {
            start: time_start,
            end: time_end
        }
    });

    // Expect aligned range read to come from cache
    // blocks     :  |    b0(cached)    |    b1(cached)    |    b2(cached)    |    b3(cached)     |   .....
    // read range :                     <------------------>
    range_size = block_size;
    start = block_size;
    end = start + range_size - 1;
    await ns_context.validate_range_read({
        type, file_name, cloud_obj_md,
        start, end,
        expect_read_size: range_size,
        expect_num_parts: 4,
        expect_upload_size: block_size * 4,
        cache_last_valid_time_range: {
            start: time_start,
            end: time_end
        }
    });

    // Expect all old cached ranges are deleted after file is changed in hub
    // blocks     :  |       b0         |  b1(to be cached) | b2(to be cached) |  b3(to be cached) |   .....
    // read range :                       <-------------------------------------->
    await ns_context.upload_directly_to_cloud({ type, file_name });
    console.log(`Waiting for TTL to expire in ${delay_ms}ms......`);
    await P.delay(delay_ms);
    range_size = (block_size * 2) + 100;
    start = block_size + 100;
    end = start + range_size - 1;
    time_start = (new Date()).getTime();
    const { noobaa_md } = await ns_context.validate_range_read({
        type, file_name, cloud_obj_md,
        start, end,
        expect_read_size: range_size,
        expect_num_parts: 1,
        expect_upload_size: block_size * 3,
        cache_last_valid_time_range: {
            start: time_start,
            end: null
        }
    });

    // Delete file from hub before TTL expires. Cache shall return the old cached range
    // blocks     :  |       b0         |     b1(cached)    |    b2(cached)    |     b3(cached)    |   .....
    // read range :                       <-------------------------------------->
    console.log(`Double check cached range is returned after ${file_name} is deleted from hub bucket and before TTL expires`);
    await ns_context.delete_file_from_cloud(type, file_name);
    await ns_context.get_object_via_cloud_expect_not_found(type, file_name);
    range_size = (block_size * 2) + 100;
    start = block_size + 100;
    end = start + range_size - 1;
    const noobaa_md_again = await ns_context.get_range_md5_size_via_noobaa(type, file_name, start, end);
    if (!_.isEqual(noobaa_md, noobaa_md_again)) {
        throw new Error(`Unexpected range read results: expected ${JSON.stringify(noobaa_md)} but got ${JSON.stringify(noobaa_md_again)}`);
    }
}

async function run_namespace_cache_tests_range_read({ type, ns_context }) {
    await ns_context.run_test_case('range read: initial read size is < block_size and not across block boundary', type,
        async () => {
            await test_case_range_read_initial_read_size_not_across_blocks({ type, ns_context });
        }
    );
}

module.exports.test_scenarios = test_scenarios;
module.exports.run = run_namespace_cache_tests_range_read;