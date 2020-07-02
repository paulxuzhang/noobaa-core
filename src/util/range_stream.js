/* Copyright (C) 2016 NooBaa */
'use strict';

var stream = require('stream');

/**
 *
 * RangeStream
 *
 * A transforming stream that returns range of the input.
 *
 */
class RangeStream extends stream.Transform {

    // The input end is exclusive
    constructor(start, end, options) {
        super(options);
        this._start = start;
        this._end = end;
        this._size = end - start;
        this._recvd_bytes = 0;
        this._sent_bytes = 0;
    }

    /**
     * implement the stream's Transform._transform() function.
     */
    _transform(data, encoding, callback) {
        if (this._sent_bytes === this._size) {
            this.end();
            return callback();
        }
        if (data && data.length) {
            if (this._sent_bytes === 0) {
                const buf = data.slice(this._start, this._end);
                if (buf.length === 0) {
                    this._start -= data.length;
                } else {
                    this.push(buf);
                    this._sent_bytes += buf.length;
                }
                this._end -= data.length;
            } else {
                const buf = data.slice(0, this._end);
                this.push(buf);
                this._sent_bytes += buf.length;
                this._end -= data.length;
            }
        }
        callback();
    }
}

module.exports = RangeStream;
