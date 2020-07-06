/* Copyright (C) 2016 NooBaa */
'use strict';

class ObjectAPIFunctions {

    constructor(client) {
        this._client = client;
    }

    async getObjectMD(params) {
        console.log('Getting object md', params);
        const { bucket, key } = params;
        try {
            const md = await this._client.object.read_object_md({
                key,
                bucket
            });

            console.log('Got object md', md);
            return md;
        } catch (e) {
            console.error('Failed to read object md', e);
            throw e;
        }
    }
}

exports.ObjectAPIFunctions = ObjectAPIFunctions;
