/* Copyright (C) 2016 NooBaa */
/*eslint max-lines-per-function: ["error", 550]*/
'use strict';

const mocha = require('mocha');
const assert = require('assert');
const RangeStream = require('../../util/range_stream');

mocha.describe('range_stream', function() {

    mocha.it('range in the second buffer of two buffers', async function() {
        const bufs = await _range_stream(8, 10, [
            new Uint8Array([11, 12, 13, 14, 15, 16]),
            new Uint8Array([17, 18, 19, 20, 21]),
            []
        ]);

        assert.strictEqual(bufs.length, 1);
        assert.strictEqual(bufs[0].length, 2);
        assert.strictEqual(bufs[0][0], 19);
        assert.strictEqual(bufs[0][1], 20);
    });

    mocha.it('range in the first buffer', async function() {
        const bufs = await _range_stream(1, 5, [
            new Uint8Array([11, 12, 13, 14, 15, 16]),
            new Uint8Array([17, 18, 19, 20, 21])
        ]);

        assert.strictEqual(bufs.length, 1);
        assert.strictEqual(bufs[0].length, 4);
        assert.strictEqual(bufs[0][0], 12);
        assert.strictEqual(bufs[0][1], 13);
        assert.strictEqual(bufs[0][2], 14);
        assert.strictEqual(bufs[0][3], 15);
    });

    mocha.it('range has one byte', async function() {
        const bufs = await _range_stream(0, 1, [
            new Uint8Array([11, 12, 13, 14, 15, 16]),
            new Uint8Array([17, 18, 19, 20, 21]),
            []
        ]);

        assert.strictEqual(bufs.length, 1);
        assert.strictEqual(bufs[0].length, 1);
        assert.strictEqual(bufs[0][0], 11);
    });

    mocha.it('range is entire buffer', async function() {
        const bufs = await _range_stream(6, 11, [
            new Uint8Array([11, 12, 13, 14, 15, 16]),
            new Uint8Array([17, 18, 19, 20, 21]),
            []
        ]);

        assert.strictEqual(bufs.length, 1);
        assert.strictEqual(bufs[0].length, 5);
        assert.strictEqual(bufs[0][0], 17);
        assert.strictEqual(bufs[0][1], 18);
        assert.strictEqual(bufs[0][2], 19);
        assert.strictEqual(bufs[0][3], 20);
        assert.strictEqual(bufs[0][4], 21);
    });

    mocha.it('range across two buffers', async function() {
        const bufs = await _range_stream(2, 9, [
            new Uint8Array([11, 12, 13, 14, 15, 16]),
            new Uint8Array([17, 18, 19, 20, 21]),
            []
        ]);

        assert.strictEqual(bufs.length, 2);
        assert.strictEqual(bufs[0].length, 4);
        assert.strictEqual(bufs[1].length, 3);
        assert.strictEqual(bufs[0][0], 13);
        assert.strictEqual(bufs[0][1], 14);
        assert.strictEqual(bufs[0][2], 15);
        assert.strictEqual(bufs[0][3], 16);
        assert.strictEqual(bufs[1][0], 17);
        assert.strictEqual(bufs[1][1], 18);
        assert.strictEqual(bufs[1][2], 19);
    });

    mocha.it('range starts from the first buffer and is larger than buffers', async function() {
        const bufs = await _range_stream(2, 100, [
            new Uint8Array([11, 12, 13, 14, 15, 16]),
            new Uint8Array([17, 18, 19, 20, 21]),
            []
        ]);

        assert.strictEqual(bufs.length, 2);
        assert.strictEqual(bufs[0].length, 4);
        assert.strictEqual(bufs[1].length, 5);
        assert.strictEqual(bufs[0][0], 13);
        assert.strictEqual(bufs[0][1], 14);
        assert.strictEqual(bufs[0][2], 15);
        assert.strictEqual(bufs[0][3], 16);
        assert.strictEqual(bufs[1][0], 17);
        assert.strictEqual(bufs[1][1], 18);
        assert.strictEqual(bufs[1][2], 19);
        assert.strictEqual(bufs[1][3], 20);
        assert.strictEqual(bufs[1][4], 21);
    });

    mocha.it('range starts from the second buffer and is larger than buffers', async function() {
        const bufs = await _range_stream(8, 100, [
            new Uint8Array([11, 12, 13, 14, 15, 16]),
            new Uint8Array([17, 18, 19, 20, 21]),
            []
        ]);

        assert.strictEqual(bufs.length, 1);
        assert.strictEqual(bufs[0].length, 3);
        assert.strictEqual(bufs[0][0], 19);
        assert.strictEqual(bufs[0][1], 20);
        assert.strictEqual(bufs[0][2], 21);
    });
});

function _range_stream(start, end, in_bufs) {
    return new Promise((resolve, reject) => {
        let bufs = [];
        let stm = new RangeStream(start, end);
        stm.on('data', data => bufs.push(data));
        stm.on('error', reject);
        stm.on('end', () => resolve(bufs));
        for (const buf of in_bufs) {
            if (buf.length === 0) {
                stm.end();
                break;
            }
            // eslint-disable-next-line no-empty-function
            stm._transform(buf, null, () => { });
        }
    });
}