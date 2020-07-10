/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const api = require('../../api');
const crypto = require('crypto');
const P = require('../../util/promise');
const promise_utils = require('../../util/promise_utils');
const { S3OPS } = require('../utils/s3ops');
const Report = require('../framework/report');
const argv = require('minimist')(process.argv);
const dbg = require('../../util/debug_module')(__filename);
const { CloudFunction } = require('../utils/cloud_functions');
const { BucketFunctions } = require('../utils/bucket_functions');
const { ObjectAPIFunctions } = require('../utils/object_api_functions');
const config = require('../../../config.js');
const { assert } = require('console');

const test_suite_name = 'namespace_cache';
dbg.set_process_name(test_suite_name);

require('../../util/dotenv').load();

const {
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    COS_ACCESS_KEY_ID,
    COS_SECRET_ACCESS_KEY,
    CACHE_TTL_MS,
    SYSTEM_NAME,
} = process.env;

const DEFAULT_CACHE_TTL_MS = 10000;
let cache_ttl_ms = CACHE_TTL_MS;

const block_size = config.NAMESPACE_CACHING.DEFAULT_BLOCK_SIZE;
const block_size_kb = block_size / 1024;
const small_file_size_kb = block_size_kb / 2;
assert(small_file_size_kb > 0);


const cloud_list = [];
if (AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY) {
    cloud_list.push('AWS');
}

if (COS_ACCESS_KEY_ID && COS_SECRET_ACCESS_KEY) {
    cloud_list.push('COS');
}

if (cloud_list.length === 0) {
    console.warn(`Missing cloud credentials, Exiting.`);
    process.exit(0);
}

const failure_test_cases = [];

//define colors
const YELLOW = "\x1b[33;1m";
const GREEN = "\x1b[32;1m";
const RED = "\x1b[31;1m";
const NC = "\x1b[0m";

const files_cloud = {
    files_AWS: [],
    files_COS: []
};

//defining the required parameters
const {
    mgmt_ip,
    mgmt_port_https,
    s3_ip,
    s3_port,
    skip_clean = false,
    clean_start = true,
    help = false
} = argv;

function usage() {
    console.log(`
    --mgmt_ip           -   noobaa management ip.
    --mgmt_port_https   -   noobaa server management https port
    --s3_ip             -   noobaa s3 ip
    --s3_port           -   noobaa s3 port
    --skip_clean        -   skipping cleaning env
    --clean_start       -   fail the test if connection, resource or bucket exists
    --help              -   show this help.
    `);
}

if (help) {
    usage();
    process.exit(1);
}

const rpc = api.new_rpc_from_base_address(`wss://${mgmt_ip}:${mgmt_port_https}`, 'EXTERNAL');
const client = rpc.new_client({});

const cf = new CloudFunction(client);
const bucket_functions = new BucketFunctions(client);
const object_functions = new ObjectAPIFunctions(client);

const aws_connection = cf.getAWSConnection();
const cos_connection = cf.getCOSConnection();

const s3ops_nb = new S3OPS({ ip: s3_ip, port: s3_port, use_https: false, sig_ver: 'v2',
        access_key: 'kD7qH0XqBJPoWTLxkRF5', secret_key: '+Zn085rPHJiASbGAwvIoEg0Iok3t3NW6gbHGszLN'});
const s3ops_aws = new S3OPS({
    ip: 's3.amazonaws.com',
    access_key: aws_connection.identity,
    secret_key: aws_connection.secret,
    system_verify_name: 'AWS',
});
const s3ops_cos = new S3OPS({
    ip: 's3.us-east.cloud-object-storage.appdomain.cloud',
    access_key: cos_connection.identity,
    secret_key: cos_connection.secret,
    system_verify_name: 'COS',
});

const connections_mapping = { COS: cos_connection, AWS: aws_connection };

//variables for using creating namespace resource
const namespace_mapping = {
    AWS: {
        s3ops: s3ops_aws,
        pool: 'cloud-resource-aws',
        bucket1: 'QA-Bucket',
        bucket2: 'qa-aws-bucket',
        namespace: 'aws-resource-namespace',
        gateway: 'aws-gateway-bucket'
    },
    COS: {
        s3ops: s3ops_cos,
        pool: 'cloud-resource-cos',
        bucket1: 'nb-ft-test1',
        bucket2: 'nb-ft-test2',
        namespace: 'cos-resource-namespace',
        gateway: 'cos-gateway-bucket'
    }
};

const test_scenarios = [
    'object cached during read to namespace bucket',
    'object cached during upload to namespace bucket',
    'cache_last_valid_time gets updated after ttl expires',
    'cache_last_valid_time will not be updated after out-of-band upload and before ttl expires',
    'cache_last_valid_time and etag get updated after ttl expires and out-of-band upload',
    'object removed from hub after ttl expires',
    'get operation: object not found',
    'delete operation success',
    'delete non-exist object',

    'range read: initial read size is < block_size and not across block boundary',

    'delete object from namespace bucket via noobaa endpoint',
    'create external connection',
    'delete external connection',
    'create namespace resource',
    'delete namespace resource',
    'create namespace bucket with caching enabled',
    'delete namespace bucket with caching enabled',
];

function _test_name(s, cloud_provider) {
    return `${s} (cloud provider: ${cloud_provider})`;
}

const report = new Report();

const test_cases = [];
const test_conf = {};
for (const t of test_scenarios) {
    for (const cloud of cloud_list) {
        test_cases.push(_test_name(t, cloud));
        test_conf[cloud] = true;
    }
}

report.init_reporter({ suite: test_suite_name, conf: test_conf, mongo_report: true, cases: test_cases });

const baseUnit = 1024;
const unit_mapping = {
    KB: {
        data_multiplier: baseUnit ** 1,
        dataset_multiplier: baseUnit ** 2
    },
    MB: {
        data_multiplier: baseUnit ** 2,
        dataset_multiplier: baseUnit ** 1
    },
    GB: {
        data_multiplier: baseUnit ** 3,
        dataset_multiplier: baseUnit ** 0
    }
};

async function get_object_s3_md(s3ops_arg, bucket, file_name, get_from_cache) {
    try {
        const ret = await s3ops_arg.get_object(bucket, file_name, get_from_cache ? { get_from_cache: true } : undefined);
        return {
            md5: crypto.createHash('md5').update(ret.Body).digest('base64'),
            size: ret.Body.length,
            etag: ret.ETag,
        };
    } catch (err) {
        throw new Error(`Failed to get data from ${file_name} in ${bucket}: ${err}`);
    }
}

async function get_object_expect_not_found(s3ops_arg, bucket, file_name) {
    try {
        const ret = await s3ops_arg.get_object(bucket, file_name);
        throw new Error(`Expect file ${file_name} not found in ${bucket}, but found with size: ${ret.ContentLength}`);
    } catch (err) {
        if (err.code === 'NoSuchKey') return true;
        throw err;
    }
}

async function valid_cache_object_noobaa_md({ noobaa_bucket, file_name, validation_params }) {
    const md = await object_functions.getObjectMD({ bucket: noobaa_bucket, key: file_name });
    const { cache_last_valid_time_range, partial_object, num_parts, size, etag, upload_size } = validation_params;
    if (cache_last_valid_time_range) {
        if (cache_last_valid_time_range.end) {
            if (!_.inRange(md.cache_last_valid_time, cache_last_valid_time_range.start, cache_last_valid_time_range.end)) {
                const msg = `expect it between ${cache_last_valid_time_range.start} and ${cache_last_valid_time_range.end}, but got ${md.cache_last_valid_time}`;
                throw new Error(`Unexpected cache_last_valid_time in object md ${file_name} from bucket ${noobaa_bucket}: ${msg}`);
            }
        } else if (md.cache_last_valid_time <= cache_last_valid_time_range.start) {
            const msg = `expect it after ${cache_last_valid_time_range.start}, but got ${md.cache_last_valid_time}`;
            throw new Error(`Unexpected cache_last_valid_time in object md ${file_name} from bucket ${noobaa_bucket}: ${msg}`);
        }
    }
    for (const [k, v] of Object.entries({ partial_object, num_parts, size, etag, upload_size })) {
        if (!_.isUndefined(v)) {
            console.log(`Validating ${k}: expect ${v}, has ${md[k]} in md`);
            assert(v === md[k]);
        }
    }
    return md;
}

async function validate_md5_between_hub_and_cache({ type, cloud_bucket, noobaa_bucket, force_cache_read, file_name, expect_same }) {
    console.log(`Comparing NooBaa cache bucket to ${type} bucket for ${file_name}`);
    const cloud_md = await get_object_s3_md(namespace_mapping[type].s3ops, cloud_bucket, file_name, false);
    const cloud_md5 = cloud_md.md5;
    const noobaa_md = await get_object_s3_md(s3ops_nb, noobaa_bucket, file_name, force_cache_read);
    const noobaa_md5 = noobaa_md.md5;
    console.log(`Noobaa cache bucket (${noobaa_bucket}) has md5 ${
        noobaa_md5} and the hub bucket ${cloud_bucket} has md5 ${cloud_md5} for file ${file_name}`);
    console.log(`file: ${file_name} size is ${cloud_md.size} on ${
            type} and ${noobaa_md.size} on noobaa`);

    if (expect_same && cloud_md5 !== noobaa_md5) {
        throw new Error(`Expect md5 ${noobaa_md5} in NooBaa cache bucket (${noobaa_bucket}) is the same as md5 ${
            cloud_md5} in hub bucket ${cloud_bucket} for file ${file_name}`);
    } else if (!expect_same && cloud_md5 === noobaa_md5) {
        throw new Error(`Expect md5 ${noobaa_md5} in NooBaa cache bucket (${noobaa_bucket}) is different than md5 ${
            cloud_md5} in hub bucket ${cloud_bucket} for file ${file_name}`);
    }

    return { cloud_md, noobaa_md };
}

// end is inclusive
async function get_range_md5_size(s3ops_arg, bucket, file_name, start, end) {
    try {
        const ret_body = await s3ops_arg.get_object_range(bucket, file_name, start, end);
        return {
            md5: crypto.createHash('md5').update(ret_body).digest('base64'),
            size: ret_body.length
        };
    } catch (err) {
        throw new Error(`Failed to get range data from ${file_name} in ${bucket}: ${err}`);
    }
}

async function validate_md5_range_read_between_hub_and_cache({ type, cloud_bucket, noobaa_bucket, file_name,
    start, end, expect_read_size, expect_same }) {

    console.log(`Comparing NooBaa cache bucket to ${type} bucket for range ${start}-${end} in ${file_name}`);
    const cloud_md = await get_range_md5_size(namespace_mapping[type].s3ops, cloud_bucket, file_name, start, end);
    const cloud_md5 = cloud_md.md5;
    const noobaa_md = await get_range_md5_size(s3ops_nb, noobaa_bucket, file_name, start, end);
    const noobaa_md5 = noobaa_md.md5;
    console.log(`Noobaa cache bucket (${noobaa_bucket}) contains the md5 ${
        noobaa_md5} and the hub bucket ${cloud_bucket} has md5 ${cloud_md5} for range ${start}-${end} in file ${file_name}`);
    console.log(`${file_name}: read range size is ${cloud_md.size} on ${type} and ${noobaa_md.size} on noobaa`);

    if (expect_same) {
        if (cloud_md5 !== noobaa_md5) {
            throw new Error(`Expect md5 ${noobaa_md5} in NooBaa cache bucket (${noobaa_bucket}) is the same as md5 ${
                cloud_md5} in hub bucket ${cloud_bucket} for range ${start}-${end} in file ${file_name}`);
        }
        if (expect_read_size !== noobaa_md.size) {
            throw new Error(`Expect range read size ${expect_read_size} on file ${file_name} from NooBaa cache bucket (${noobaa_bucket}) is ${
                noobaa_md.size} for read range ${start}-${end}`);
        }
    } else if (!expect_same && cloud_md5 === noobaa_md5) {
        throw new Error(`Expect md5 ${noobaa_md5} in NooBaa cache bucket (${noobaa_bucket}) is different than md5 ${
            cloud_md5} in hub bucket ${cloud_bucket} for range ${start}-${end} in file ${file_name}`);
    }
    return { cloud_md, noobaa_md };
}

/*async function isFilesAvailableInNooBaaBucket(gateway, files, type) {
    console.log(`Checking uploaded files ${files} in noobaa s3 server bucket ${gateway}`);
    const list_files = await s3ops_nb.get_list_files(gateway);
    const keys = list_files.map(key => key.Key);
    let report_fail = false;
    for (const file of files) {
        if (keys.includes(file)) {
            console.log('Server contains file ' + file);
        } else {
            report_fail = true;
            report.fail(`verify list files ${type}`);
            throw new Error(`Server is not contains uploaded file ${file} in bucket ${gateway}`);
        }
    }
    if (!report_fail) {
        report.success(`verify list files ${type}`);
    }
}*/

async function set_rpc_and_create_auth_token() {
    const auth_params = {
        email: 'demo@noobaa.com',
        password: 'DeMo1',
        system: SYSTEM_NAME ? SYSTEM_NAME : 'demo'
    };
    return client.create_auth_token(auth_params);
}

// file_name follows the pattern prefix_name_[0-9]+_[KB|MB|GB]
function _get_size_from_file_name(file_name) {
    const tokens = file_name.split('_');
    if (tokens.length < 2) {
        return { size: 1, data_multiplier: unit_mapping.KB.data_multiplier };
    } else {
        const { data_multiplier } = _.defaultTo(unit_mapping[_.last(tokens)], unit_mapping.KB);
        const size = _.toInteger(tokens[tokens.length - 2]);
        return { size: size === 0 ? 1 : size, data_multiplier };
    }
}

async function upload_via_noobaa_endpoint({ type, file_name, bucket }) {
    const { size, data_multiplier } = _get_size_from_file_name(file_name);
    if (!file_name) {
        file_name = 'file_namespace_test_' + (Math.floor(Date.now() / 1000));
    }
    if (!bucket) {
        bucket = namespace_mapping[type].gateway;
    }
    console.log(`uploading ${file_name} via noobaa bucket ${bucket}`);
    files_cloud[`files_${type}`].push(file_name);
    try {
        await s3ops_nb.put_file_with_md5(bucket, file_name, size, data_multiplier);
    } catch (err) {
        throw new Error(`Failed upload file ${file_name} ${err}`);
    }
    return file_name;
}

async function upload_directly_to_cloud({ type, file_name }) {
    const { size, data_multiplier } = _get_size_from_file_name(file_name);
    if (!file_name) {
        file_name = 'file_namespace_test_' + (Math.floor(Date.now() / 1000));
    }
    const hub_bucket = namespace_mapping[type].bucket2;
    console.log(`uploading ${file_name} directly to ${type} bucket ${hub_bucket}`);
    files_cloud[`files_${type}`].push(file_name);
    try {
        const md5 = await namespace_mapping[type].s3ops.put_file_with_md5(hub_bucket, file_name, size, data_multiplier);
        return md5;
    } catch (err) {
        throw new Error(`Failed to upload directly into ${type} bucket ${hub_bucket}`);
    }
}

async function list_files_in_cloud(type) {
    const list_files_obj = await namespace_mapping[type].s3ops.get_list_files(namespace_mapping[type].bucket2);
    return list_files_obj.map(file => file.Key);
}

async function check_via_cloud(type, file_name) {
    console.log(`checking via ${type}: ${namespace_mapping[type].bucket2}`);
    const list_files = await list_files_in_cloud(type);
    console.log(`${type} files list ${list_files}`);
    if (list_files.includes(file_name)) {
        console.log(`${file_name} was uploaded via noobaa and found via ${type}`);
    } else {
        throw new Error(`${file_name} was uploaded via noobaa and was not found via ${type}`);
    }
    return true;
}

async function validate_range_read({ type, cloud_bucket, noobaa_bucket, file_name,
    cloud_obj_md, start, end, cache_last_valid_time_range,
    expect_read_size, expect_num_parts, expect_upload_size }) {

    console.log(`Reading range ${start}-${end} in ${file_name}`);
    const mds = await validate_md5_range_read_between_hub_and_cache({
        type,
        cloud_bucket,
        noobaa_bucket,
        file_name,
        start,
        end,
        expect_read_size,
        expect_same: true
    });
    await promise_utils.wait_until(async () => {
        try {
            await valid_cache_object_noobaa_md({
                noobaa_bucket,
                file_name,
                validation_params: {
                    cache_last_valid_time_range,
                    size: cloud_obj_md.size,
                    etag: cloud_obj_md.etag,
                    partial_object: true,
                    num_parts: expect_num_parts,
                    upload_size: expect_upload_size,
                }
            });
            return true;
        } catch (err) {
            if (err.rpc_code === 'NO_SUCH_OBJECT') return false;
            throw err;
        }
    }, 10000);

    return mds;
}

async function _run_test_case(test_name, test_case_fn) {
    console.log(`+++ ${YELLOW}running test case: ${test_name}${NC}`);
    try {
        await test_case_fn();
        console.log(`--- ${GREEN}test case passed: ${test_name}${NC}`);
        report.success(test_name);
    } catch (err) {
        console.log(`!!! ${RED}test case (${test_name}) failed${NC}: ${err}`);
        failure_test_cases.push({ test_name, err: `${err}` });
        report.fail(test_name);
    }
}

async function run_namespace_cache_tests_non_range_read(type) {
    const cloud_s3_ops = namespace_mapping[type].s3ops;
    const noobaa_bucket = namespace_mapping[type].gateway;
    const cloud_bucket = namespace_mapping[type].bucket2;

    const delay_ms = cache_ttl_ms + 1000;
    // file size MUST be larger than the size of first range data, i.e. config.INLINE_MAX_SIZE
    const min_file_size_kb = (config.INLINE_MAX_SIZE / 1024) + 1;
    const prefix = `file_${(Math.floor(Date.now() / 1000))}_${type}`;
    const file_name1 = `${prefix}_${min_file_size_kb * 2}_KB`;
    const file_name2 = `${prefix}_${min_file_size_kb + 1}_KB`;
    const file_name_delete_case1 = `delete_${prefix}_${min_file_size_kb + 1}_KB`;
    const file_name_delete_case2 = `delete_${prefix}_${min_file_size_kb + 2}_KB`;
    let cache_last_valid_time;
    let time_start = (new Date()).getTime();

    await _run_test_case(_test_name('object cached during upload to namespace bucket', type), async () => {
        // Upload a file to namespace cache bucket
        // Expect that etags in both hub and noobaa cache bucket match
        // Expect that cache_last_valid_time is set in object MD
        const file_name = file_name1;
        await upload_via_noobaa_endpoint({ type, file_name });
        await check_via_cloud(type, file_name1);
        await validate_md5_between_hub_and_cache({
            type,
            cloud_bucket,
            noobaa_bucket,
            force_cache_read: true,
            file_name,
            expect_same: true
        });
        await validate_md5_between_hub_and_cache({
            type,
            cloud_bucket,
            noobaa_bucket,
            force_cache_read: false,
            file_name,
            expect_same: true
        });
        await valid_cache_object_noobaa_md({
            noobaa_bucket,
            file_name,
            validation_params: {
                cache_last_valid_time_range: {
                    start: time_start,
                    end: (new Date()).getTime()
                }
            }
        });
    });

    await _run_test_case(_test_name('cache_last_valid_time gets updated after ttl expires', type), async () => {
        // Wait for cache TTL to expire and read the file again
        // Expect cache_last_valid_time to be updated in object MD
        const file_name = file_name1;
        console.log(`Waiting for TTL to expire in ${delay_ms}ms......`);
        await P.delay(delay_ms);
        time_start = (new Date()).getTime();
        await s3ops_nb.get_file_check_md5(noobaa_bucket, file_name);
        const md = await valid_cache_object_noobaa_md({
            noobaa_bucket,
            file_name,
            validation_params: {
                cache_last_valid_time_range: {
                    start: time_start,
                    end: (new Date()).getTime()
                }
            }
        });
        cache_last_valid_time = md.cache_last_valid_time;
    });

    await _run_test_case(_test_name('cache_last_valid_time will not be updated after out-of-band upload and before ttl expires', type), async () => {
        // Upload the file with different content to hub before cache TTL expires
        // Expect the cached file with different etag to be returned
        // Expect cache_last_valid_time to stay the same
        const file_name = file_name1;
        await upload_directly_to_cloud({ type, file_name });
        await validate_md5_between_hub_and_cache({
            type,
            cloud_bucket,
            noobaa_bucket,
            force_cache_read: false,
            file_name,
            expect_same: false
        });
        await valid_cache_object_noobaa_md({
            noobaa_bucket,
            file_name,
            validation_params: {
                cache_last_valid_time_range: {
                    start: cache_last_valid_time - 1,
                    end: cache_last_valid_time + 1
                }
            }
        });
    });

    await _run_test_case(_test_name('cache_last_valid_time and etag get updated after ttl expires and out-of-band upload', type), async () => {
        // Wait for cache TTL to expire
        // Expect that etags in both hub and noobaa cache bucket match
        // Expect that cache_last_valid_time is updated in object MD
        const file_name = file_name1;
        console.log(`Waiting for TTL to expire in ${delay_ms}ms......`);
        await P.delay(delay_ms);
        time_start = (new Date()).getTime();
        await validate_md5_between_hub_and_cache({
            type,
            cloud_bucket,
            noobaa_bucket,
            force_cache_read: false,
            file_name,
            expect_same: true
        });
        await validate_md5_between_hub_and_cache({
            type,
            cloud_bucket,
            noobaa_bucket,
            force_cache_read: true,
            file_name,
            expect_same: true
        });
        await valid_cache_object_noobaa_md({
            noobaa_bucket,
            file_name,
            validation_params: {
                cache_last_valid_time_range: {
                    start: time_start,
                    end: (new Date()).getTime()
                }
            }
        });
    });

    await _run_test_case(_test_name('object cached during read to namespace bucket', type), async () => {
        // Upload a file to hub bucket and read it from namespace bucket
        // Expect that etags in both hub and noobaa cache bucket match
        // Expect that cache_last_valid_time is set in object MD
        const file_name = file_name2;
        await upload_directly_to_cloud({ type, file_name });
        time_start = (new Date()).getTime();
        await validate_md5_between_hub_and_cache({
            type,
            cloud_bucket,
            noobaa_bucket,
            force_cache_read: false,
            file_name,
            expect_same: true
        });
        await promise_utils.wait_until(async () => {
            try {
                await valid_cache_object_noobaa_md({
                    noobaa_bucket,
                    file_name,
                    validation_params: {
                        cache_last_valid_time_range: {
                            start: time_start,
                            end: (new Date()).getTime()
                        }
                    }
                });
                return true;
            } catch (err) {
                if (err.rpc_code === 'NO_SUCH_OBJECT') return false;
                throw err;
            }
        }, 10000);
    });

    await _run_test_case(_test_name('object removed from hub after ttl expires', type), async () => {
        // Upload a file to cache bucket and delete it from hub bucket
        // Expect 404 to be returned for read from cache bucket after TTL expires
        const file_name = file_name_delete_case1;
        await upload_via_noobaa_endpoint({ type, file_name });
        await validate_md5_between_hub_and_cache({
            type,
            cloud_bucket,
            noobaa_bucket,
            force_cache_read: true,
            file_name,
            expect_same: true
        });
        console.log(`Waiting for TTL to expire in ${delay_ms}ms......`);
        await P.delay(delay_ms);
        await cloud_s3_ops.delete_file(cloud_bucket, file_name);
        await get_object_expect_not_found(s3ops_nb, noobaa_bucket, file_name);
    });

    await _run_test_case(_test_name('delete operation success', type), async () => {
        const file_name = file_name_delete_case2;
        await upload_via_noobaa_endpoint({ type, file_name });
        await validate_md5_between_hub_and_cache({
            type,
            cloud_bucket,
            noobaa_bucket,
            force_cache_read: true,
            file_name,
            expect_same: true
        });
        await s3ops_nb.delete_file(noobaa_bucket, file_name);
        await get_object_expect_not_found(s3ops_nb, noobaa_bucket, file_name);
        await get_object_expect_not_found(cloud_s3_ops, cloud_bucket, file_name);
    });

    await _run_test_case(_test_name('get operation: object not found', type), async () => {
        const file_name = 'file_not_exist_123';
        await get_object_expect_not_found(s3ops_nb, noobaa_bucket, file_name);
    });

    await _run_test_case(_test_name('delete non-exist object', type), async () => {
        const file_name = 'file_not_exist_123';
        await s3ops_nb.delete_file(noobaa_bucket, file_name);
    });

}

async function test_case_range_read_initial_read_size_not_across_blocks({
    type, cloud_s3_ops, cloud_bucket, noobaa_bucket}) {

    // Make file big enough for holding multiple blocks
    const prefix = `file_${(Math.floor(Date.now() / 1000))}_${type}`;
    const file_name = `${prefix}_${block_size_kb * 5}_KB`;
    const delay_ms = cache_ttl_ms + 1000;
    let time_start = (new Date()).getTime();

    // Expect block3 will be cached
    // blocks     :  |       b0        |       b1         |       b2      |  b3(to be cached) |   .....
    // read range :                                                         <-->
    let range_size = 100;
    let start = (block_size * 3) + 100;
    let end = start + range_size - 1;

    await upload_directly_to_cloud({ type, file_name });
    console.log(`Reading range ${start}-${end} in ${file_name}`);
    await validate_md5_range_read_between_hub_and_cache({
        type,
        cloud_bucket,
        noobaa_bucket,
        file_name,
        start,
        end,
        expect_read_size: range_size,
        expect_same: true
    });
    const cloud_obj_md = get_object_s3_md(cloud_s3_ops, cloud_bucket, file_name);
    let time_end = (new Date()).getTime();
    await promise_utils.wait_until(async () => {
        try {
            await valid_cache_object_noobaa_md({
                noobaa_bucket,
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
        await validate_range_read({
            type, cloud_bucket, noobaa_bucket, file_name, cloud_obj_md,
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
    await validate_range_read({
        type, cloud_bucket, noobaa_bucket, file_name, cloud_obj_md,
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
    await validate_range_read({
        type, cloud_bucket, noobaa_bucket, file_name, cloud_obj_md,
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
    await validate_range_read({
        type, cloud_bucket, noobaa_bucket, file_name, cloud_obj_md,
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
    await upload_directly_to_cloud({ type, file_name });
    console.log(`Waiting for TTL to expire in ${delay_ms}ms......`);
    await P.delay(delay_ms);
    range_size = (block_size * 2) + 100;
    start = block_size + 100;
    end = start + range_size - 1;
    time_start = (new Date()).getTime();
    const { noobaa_md } = await validate_range_read({
        type, cloud_bucket, noobaa_bucket, file_name, cloud_obj_md,
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
    console.log(`Double check cached range is returned after ${file_name} is deleted from hub ${cloud_bucket} and before TTL expires`);
    await cloud_s3_ops.delete_file(cloud_bucket, file_name);
    await get_object_expect_not_found(cloud_s3_ops, cloud_bucket, file_name);
    range_size = (block_size * 2) + 100;
    start = block_size + 100;
    end = start + range_size - 1;
    const noobaa_md_again = await get_range_md5_size(s3ops_nb, noobaa_bucket, file_name, start, end);
    if (!_.isEqual(noobaa_md, noobaa_md_again)) {
        throw new Error(`Unexpected range read results: expected ${JSON.stringify(noobaa_md)} but got ${JSON.stringify(noobaa_md_again)}`);
    }
}

async function run_namespace_cache_tests_range_read(type) {
    const cloud_s3_ops = namespace_mapping[type].s3ops;
    const noobaa_bucket = namespace_mapping[type].gateway;
    const cloud_bucket = namespace_mapping[type].bucket2;

    await _run_test_case(
        _test_name('range read: initial read size is < block_size and not across block boundary', type),
        async () => {
            await test_case_range_read_initial_read_size_not_across_blocks({ type, cloud_s3_ops, cloud_bucket, noobaa_bucket });
        }
    );
}

async function create_account_resources(type) {
    let connection_name;
    try {
        connection_name = await cf.getConnection(connections_mapping[type].endpoint);
        if (connection_name) {
            console.log(`connection ${connections_mapping[type].endpoint} exists under the name ${connection_name}`);
        } else {
            //create connection
            await cf.createConnection(connections_mapping[type], type);
            connection_name = connections_mapping[type].name;
        }
        report.success(_test_name('create external connection', type));
    } catch (e) {
        report.fail(_test_name('create external connection', type));
        if (!clean_start && e.rpc_code !== 'CONNECTION_ALREADY_EXIST') {
            throw new Error(e);
        }
    }
    try {
        // create namespace resource
        await cf.createNamespaceResource(connection_name,
            namespace_mapping[type].namespace, namespace_mapping[type].bucket2);
        report.success(_test_name('create namespace resource', type));
    } catch (e) {
        report.fail(_test_name('create namespace resource', type));
        if (!clean_start && e.rpc_code !== 'IN_USE') {
            throw new Error(e);
        }
    }
    try {
        //create a namespace bucket
        cache_ttl_ms = _.defaultTo(CACHE_TTL_MS, DEFAULT_CACHE_TTL_MS);
        await bucket_functions.createNamespaceBucket(namespace_mapping[type].gateway,
            namespace_mapping[type].namespace, { ttl_ms: cache_ttl_ms });
        report.success(_test_name('create namespace bucket with caching enabled', type));
    } catch (e) {
        console.log("error:", e, "==", e.rpc_code);
        report.fail(_test_name('create namespace bucket with caching enabled', type));
        if (!clean_start && e.rpc_code !== 'BUCKET_ALREADY_OWNED_BY_YOU') {
            throw new Error(e);
        }
    }
}

async function delete_account_resources(clouds) {
    for (const type of clouds) {
        try {
            await cf.deleteNamespaceResource(namespace_mapping[type].namespace);
            report.success(_test_name('delete namespace resource', type));
        } catch (err) {
            report.fail(_test_name('delete namespace resource', type));
        }
        /*try {
            await cf.deleteConnection(connections_mapping[type].name);
            report.success(_test_name('delete external connection', type));
        } catch (err) {
            report.fail(_test_name('delete external connection', type));
        }*/
    }
}

async function delete_namespace_bucket(bucket, type) {
    console.log('Deleting namespace bucket ' + bucket);
    await clean_namespace_bucket(bucket, type);

    try {
        await bucket_functions.deleteBucket(bucket);
        report.success(_test_name('delete namespace bucket with caching enabled', type));
    } catch (err) {
        report.fail(_test_name('delete namespace bucket with caching enabled', type));
        throw new Error(`Failed to delete namespace bucket ${bucket} with error ${err}`);
    }
}

async function clean_namespace_bucket(bucket, type) {
    const list_files = await s3ops_nb.get_list_files(bucket);
    const keys = list_files.map(key => key.Key);
    if (keys) {
        for (const file of keys) {
            try {
                await s3ops_nb.delete_file(bucket, file);
            } catch (e) {
                report.fail(_test_name('delete object from namespace bucket via noobaa endpoint', type));
            }
        }
        report.success(_test_name('delete object from namespace bucket via noobaa endpoint', type));
    }
}

async function main(clouds) {
    try {
        await set_rpc_and_create_auth_token();
        for (const type of clouds) {
            await create_account_resources(type);
            await run_namespace_cache_tests_non_range_read(type);
            await run_namespace_cache_tests_range_read(type);
        }
        if (!skip_clean) {
            for (const type of clouds) {
                await delete_namespace_bucket(namespace_mapping[type].gateway, type);
            }
            await delete_account_resources(clouds);
        }
        await report.report();
        if (failure_test_cases.length) {
            console.error(`${RED}Failed tests${NC}: ${JSON.stringify(failure_test_cases, null, 4)}`);
            process.exit(1);
        } else {
            console.log(`${GREEN}namespace cache tests were successful!${NC}`);
            process.exit(0);
        }
    } catch (err) {
        console.error('something went wrong', err);
        await report.report();
        process.exit(1);
    }
}

main(cloud_list);
