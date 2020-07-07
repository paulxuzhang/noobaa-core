/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const api = require('../../api');
const crypto = require('crypto');
const P = require('../../util/promise');
const { S3OPS } = require('../utils/s3ops');
const Report = require('../framework/report');
const argv = require('minimist')(process.argv);
const dbg = require('../../util/debug_module')(__filename);
const { CloudFunction } = require('../utils/cloud_functions');
const { BucketFunctions } = require('../utils/bucket_functions');
const { ObjectAPIFunctions } = require('../utils/object_api_functions');

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

const errors = [];
let failures_in_test = false;

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

const AWSDefaultConnection = cf.getAWSConnection();
const COSDefaultConnection = cf.getCOSConnection();

const s3opsNB = new S3OPS({ ip: s3_ip, port: s3_port, use_https: false, sig_ver: 'v2',
        access_key: 'vZnfG4rgxGL9NP0a2sHh', secret_key: 'V19A8Fi3gsjD7w5OxjBH9WT3XJ3sSWCtNuvQmDys'});
const s3opsAWS = new S3OPS({
    ip: 's3.amazonaws.com',
    access_key: AWSDefaultConnection.identity,
    secret_key: AWSDefaultConnection.secret,
    system_verify_name: 'AWS',
});
const s3opsCOS = new S3OPS({
    ip: 's3.us-east.cloud-object-storage.appdomain.cloud',
    access_key: COSDefaultConnection.identity,
    secret_key: COSDefaultConnection.secret,
    system_verify_name: 'COS',
});

const connections_mapping = { COS: COSDefaultConnection, AWS: AWSDefaultConnection };

//variables for using creating namespace resource
const namespace_mapping = {
    AWS: {
        s3ops: s3opsAWS,
        pool: 'cloud-resource-aws',
        bucket1: 'QA-Bucket',
        bucket2: 'qa-aws-bucket',
        namespace: 'aws-resource-namespace',
        gateway: 'aws-gateway-bucket'
    },
    COS: {
        s3ops: s3opsCOS,
        pool: 'cloud-resource-cos',
        bucket1: 'nb-ft-test1',
        bucket2: 'nb-ft-test2',
        namespace: 'cos-resource-namespace',
        gateway: 'cos-gateway-bucket'
    }
};

const test_scenarios = [
    'object cached during upload to namespace bucket',
    'cache_last_valid_time gets updated after ttl expires',
    'cache_last_valid_time will not be updated after out-of-band upload and before ttl expires',
    'cache_last_valid_time and etag get updated after ttl expires and out-of-band upload',

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

/*const dataSet = [
    { size_units: 'KB', data_size: 1 },
    { size_units: 'KB', data_size: 500 },
    { size_units: 'MB', data_size: 1 },
    { size_units: 'MB', data_size: 100 },
];*/

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

async function getPropertiesFromS3Bucket(s3ops_arg, bucket, file_name, get_from_cache) {
    try {
        const object_list = await s3ops_arg.get_object(bucket, file_name, get_from_cache ? { get_from_cache: true } : undefined);
        console.log('Getting md5 data from file ' + file_name);
        return {
            md5: crypto.createHash('md5').update(object_list.Body).digest('base64'),
            size: object_list.Body.length
        };
    } catch (err) {
        throw new Error(`Getting md5 data from file ${file_name} ${err}`);
    }
}

async function validCacheObjectMD(bucket, file_name, validation_params) {
    try {
        const md = await object_functions.getObjectMD({ bucket, key: file_name });
        const { cache_last_valid_time_range } = validation_params;
        if (!_.inRange(md.cache_last_valid_time, cache_last_valid_time_range.start, cache_last_valid_time_range.end)) {
            const msg = `expect it between ${cache_last_valid_time_range.start} and ${cache_last_valid_time_range.end}, but got ${md.cache_last_valid_time}`;
            throw new Error(`Unexpected cache_last_valid_time in object md ${file_name} from bucket ${bucket}: ${msg}`);
        }

        return md.cache_last_valid_time;
    } catch (err) {
        throw new Error(`Failed to get object md ${file_name} from bucket ${bucket}: ${err}`);
    }
}

async function compareMD5BetweenCloudAndNooBaa({ type, cloud_bucket, noobaa_bucket, force_cache_read, file_name, expect_same }) {
    console.log(`Comparing NooBaa cache bucket to ${type} bucket for ${file_name}`);
    const cloudProperties = await getPropertiesFromS3Bucket(namespace_mapping[type].s3ops, cloud_bucket, file_name, false);
    const cloudMD5 = cloudProperties.md5;
    const noobaaProperties = await getPropertiesFromS3Bucket(s3opsNB, noobaa_bucket, file_name, force_cache_read);
    const noobaaMD5 = noobaaProperties.md5;
    console.log(`Noobaa cache bucket for (${noobaa_bucket}) contains the md5 ${
        noobaaMD5} and the cloud md5 is: ${JSON.stringify(cloudProperties)} for file ${file_name}`);
    console.log(`file: ${file_name} size is ${cloudProperties.size} on ${
            type} and ${noobaaProperties.size} on noobaa`);

    if (expect_same && cloudMD5 !== noobaaMD5) {
        throw new Error(`Expect md5 ${noobaaMD5} in NooBaa cache bucket (${noobaa_bucket}) is the same as md5 ${
            cloudMD5} in the cloud hub bucket for file ${file_name}`);
    } else if (!expect_same && cloudMD5 === noobaaMD5) {
        throw new Error(`Expect md5 ${noobaaMD5} in NooBaa cache bucket (${noobaa_bucket}) is different than md5 ${
            cloudMD5} in the cloud hub bucket for file ${file_name}`);
    }
}

/*async function uploadDataSetToCloud(type, bucket) {
    for (const size of dataSet) {
        const { data_multiplier } = unit_mapping[size.size_units.toUpperCase()];
        const file_name = 'file_' + size.data_size + size.size_units + (Math.floor(Date.now() / 1000));
        files_cloud[`files_${type}`].push(file_name);
        await namespace_mapping[type].s3ops.put_file_with_md5(bucket, file_name, size.data_size, data_multiplier);
    }
}

async function isFilesAvailableInNooBaaBucket(gateway, files, type) {
    console.log(`Checking uploaded files ${files} in noobaa s3 server bucket ${gateway}`);
    const list_files = await s3opsNB.get_list_files(gateway);
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

async function uploadFileToNoobaaS3(bucket, file_name) {
    const { data_multiplier } = unit_mapping.KB;
    try {
        await s3opsNB.put_file_with_md5(bucket, file_name, 15, data_multiplier);
    } catch (err) {
        throw new Error(`Failed upload file ${file_name} ${err}`);
    }
}

async function set_rpc_and_create_auth_token() {
    const auth_params = {
        email: 'demo@noobaa.com',
        password: 'DeMo1',
        system: SYSTEM_NAME ? SYSTEM_NAME : 'demo'
    };
    return client.create_auth_token(auth_params);
}

/*async function _upload_check_cache_and_cloud(type) {
    //upload dataset
    await uploadDataSetToCloud(type, namespace_mapping[type].bucket2);
    await upload_directly_to_cloud(type);
    await isFilesAvailableInNooBaaBucket(namespace_mapping[type].gateway, files_cloud[`files_${type}`], type);
    for (const file of files_cloud[`files_${type}`]) {
        try {
            await compareMD5betweenCloudAndNooBaa(type, namespace_mapping[type].bucket2, namespace_mapping[type].gateway, false, file);
            report.success(`read via namespace ${type}`);
        } catch (err) {
            console.log('Failed upload via cloud , check via noobaa');
            throw err;
        }
    }
}*/

async function upload_directly_to_cloud({ type, file_name }) {
    const { data_multiplier } = unit_mapping.KB;
    if (!file_name) {
        file_name = 'file_namespace_test_' + (Math.floor(Date.now() / 1000));
    }
    const hub_bucket = namespace_mapping[type].bucket2;
    console.log(`uploading ${file_name} directly to ${type} bucket ${hub_bucket}`);
    files_cloud[`files_${type}`].push(file_name);
    try {
        await namespace_mapping[type].s3ops.put_file_with_md5(hub_bucket, file_name, 15, data_multiplier);
        return file_name;
    } catch (err) {
        throw new Error(`Failed to upload directly into ${type} bucket ${hub_bucket}`);
    }
}

async function upload_via_noobaa_endpoint({ type, file_name, bucket }) {
    //Try to upload a file to noobaa s3 server
    if (!file_name) {
        file_name = 'file_namespace_test_' + (Math.floor(Date.now() / 1000));
    }
    if (!bucket) {
        bucket = namespace_mapping[type].gateway;
    }
    console.log(`uploading ${file_name} via noobaa bucket ${bucket}`);
    files_cloud[`files_${type}`].push(file_name);
    await uploadFileToNoobaaS3(bucket, file_name);
    return file_name;
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

async function _run_test_case(test_name, test_case_fn) {
    console.log(`+++ ${YELLOW}running test case: ${test_name}${NC}`);
    try {
        await test_case_fn();
        console.log(`--- ${GREEN}test case passed: ${test_name}${NC}`);
        report.success(test_name);
    } catch (err) {
        console.log(`!!! ${RED}test case (${test_name}) failed${NC}: ${err}`);
        report.fail(test_name);
    }
}

async function run_namespace_cache_tests({ type, file_name, bucket }) {
    const delay_ms = cache_ttl_ms + 1000;
    let uploaded_file_name;
    let cache_last_valid_time;
    let time_start = (new Date()).getTime();

    await _run_test_case(_test_name('object cached during upload to namespace bucket', type), async () => {
        // Upload a file to namespace cache bucket
        // Expect that etags in both hub and noobaa cache bucket match
        // Expect that cache_last_valid_time is set in object MD
        uploaded_file_name = await upload_via_noobaa_endpoint({ type, file_name, bucket });
        await check_via_cloud(type, uploaded_file_name);
        await compareMD5BetweenCloudAndNooBaa({
            type,
            cloud_bucket: namespace_mapping[type].bucket2,
            noobaa_bucket: bucket,
            force_cache_read: true,
            file_name: uploaded_file_name,
            expect_same: true
        });
        await compareMD5BetweenCloudAndNooBaa({
            type,
            cloud_bucket: namespace_mapping[type].bucket2,
            noobaa_bucket: bucket,
            force_cache_read: false,
            file_name: uploaded_file_name,
            expect_same: true
        });

        await validCacheObjectMD(bucket, uploaded_file_name, {
            cache_last_valid_time_range: {
                start: time_start,
                end: (new Date()).getTime()
            }
        });
    });

    await _run_test_case(_test_name('cache_last_valid_time gets updated after ttl expires', type), async () => {
        // Wait for cache TTL to expire and read the file again
        // Expect cache_last_valid_time to be updated in object MD
        console.log(`Waiting for TTL to expire in ${delay_ms}ms......`);
        await P.delay(delay_ms);
        time_start = (new Date()).getTime();
        await s3opsNB.get_file_check_md5(bucket, uploaded_file_name);
        cache_last_valid_time = await validCacheObjectMD(bucket, uploaded_file_name, {
            cache_last_valid_time_range: {
                start: time_start,
                end: (new Date()).getTime()
            }
        });
    });

    await _run_test_case(_test_name('cache_last_valid_time will not be updated after out-of-band upload and before ttl expires', type), async () => {
        // Upload the file with different content to hub before cache TTL expires
        // Expect the cached file with different etag to be returned
        // Expect cache_last_valid_time to stay the same
        await upload_directly_to_cloud({ type, file_name: uploaded_file_name });
        await compareMD5BetweenCloudAndNooBaa({
            type,
            cloud_bucket: namespace_mapping[type].bucket2,
            noobaa_bucket: bucket,
            force_cache_read: false,
            file_name: uploaded_file_name,
            expect_same: false
        });
        await validCacheObjectMD(bucket, uploaded_file_name, {
            cache_last_valid_time_range: {
                start: cache_last_valid_time - 1,
                end: cache_last_valid_time + 1
            }
        });
    });

    await _run_test_case(_test_name('cache_last_valid_time and etag get updated after ttl expires and out-of-band upload', type), async () => {
        // Wait for cache TTL to expire
        // Expect that etags in both hub and noobaa cache bucket match
        // Expect that cache_last_valid_time is updated in object MD
        console.log(`Waiting for TTL to expire in ${delay_ms}ms......`);
        await P.delay(delay_ms);
        time_start = (new Date()).getTime();
        await compareMD5BetweenCloudAndNooBaa({
            type,
            cloud_bucket: namespace_mapping[type].bucket2,
            noobaa_bucket: bucket,
            force_cache_read: false,
            file_name: uploaded_file_name,
            expect_same: true
        });
        await compareMD5BetweenCloudAndNooBaa({
            type,
            cloud_bucket: namespace_mapping[type].bucket2,
            noobaa_bucket: bucket,
            force_cache_read: true,
            file_name: uploaded_file_name,
            expect_same: true
        });
        await validCacheObjectMD(bucket, uploaded_file_name, {
            cache_last_valid_time_range: {
                start: time_start,
                end: (new Date()).getTime()
            }
        });
    });
}

/*async function update_read_write_and_check(clouds, name, read_resources, write_resource) {
    let should_fail;
    const run_on_clouds = _.clone(clouds);
    try {
        await bucket_functions.updateNamesapceBucket(name, write_resource, read_resources);
        report.success('update namespace bucket w resource');
    } catch (e) {
        report.fail('update namespace bucket w resource');
        throw new Error(e);
    }
    await P.delay(30 * 1000);
    console.error(`${RED}TODO: REMOVE THIS DELAY, IT IS TEMP OVERRIDE FOR BUG #4831${NC}`);
    const uploaded_file_name = await upload_via_noobaa_endpoint({ type: run_on_clouds[0], bucket: name });
    //checking that the file was written into the read/write cloud
    await check_via_cloud(run_on_clouds[0], uploaded_file_name);
    run_on_clouds.shift();
    for (let cycle = 0; cycle < run_on_clouds.length; cycle++) {
        //checking that the file was not written into the read only clouds
        try {
            should_fail = await check_via_cloud(run_on_clouds[cycle], uploaded_file_name);
        } catch (e) {
            console.log(`${e}, as should`);
        }
        if (should_fail) {
            throw new Error(`Upload succeed To the read only cloud (${run_on_clouds[cycle]}) while it shouldn't`);
        }
    }
}

async function list_cloud_files_read_via_noobaa(type, noobaa_bucket) {
    const files_in_cloud_bucket = await list_files_in_cloud(type);
    for (const file of files_in_cloud_bucket) {
        try {
            await compareMD5betweenCloudAndNooBaa(type, namespace_mapping[type].bucket2, noobaa_bucket, file);
            report.success(`verify md5 via list on ${type}`);
        } catch (err) {
            report.fail(`verify md5 via list on ${type}`);
            throw err;
        }
    }
}*/

async function create_resources(type) {
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

async function delete_resources(clouds) {
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
    const list_files = await s3opsNB.get_list_files(bucket);
    const keys = list_files.map(key => key.Key);
    if (keys) {
        for (const file of keys) {
            try {
                await s3opsNB.delete_file(bucket, file);
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
            await create_resources(type);
            await run_namespace_cache_tests({ type, file: 'file_cos_15KB', bucket: namespace_mapping[type].gateway });
        }
        if (!skip_clean) {
            for (const type of clouds) {
                await delete_namespace_bucket(namespace_mapping[type].gateway, type);
            }
            await delete_resources(clouds);
        }
        await report.report();
        if (failures_in_test) {
            console.error('Errors during namespace test');
            console.error(`${JSON.stringify(_.countBy(errors), null, 4)}`);
            process.exit(1);
        } else {
            console.log('namespace cache tests were successful!');
            process.exit(0);
        }
    } catch (err) {
        console.error('something went wrong', err);
        await report.report();
        process.exit(1);
    }
}

main(cloud_list);
