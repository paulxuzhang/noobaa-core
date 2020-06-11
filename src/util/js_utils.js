/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const util = require('util');

/**
 *
 * self_bind
 *
 * create a lightweight bind which is based on simple closure of the object.
 *
 * the native Function.bind() produces a function with very slow performance,
 * the reason for that seems to be that the spec for bind() is bigger than simple closure.
 *
 * see http://stackoverflow.com/questions/17638305/why-is-bind-slower-than-a-closure
 *
 * see src/test/measure_bind_perf.js
 *
 * @param method_desc optional string or array of strings of method names
 *      to bind, if not supplied all enumerable functions will be used.
 */
function self_bind(object, method_desc) {
    if (!_.isString(method_desc)) {
        method_desc = method_desc || _.functionsIn(object);
        _.each(method_desc, function(method) {
            self_bind(object, method);
        });
        return;
    }

    var func = object[method_desc];

    // create a closure function that applies the original function on object
    function closure_func() {
        return func.apply(object, arguments);
    }

    object[method_desc] = closure_func;

    return closure_func;
}


// see http://jsperf.com/concat-vs-push-apply/39
var _cached_array_push = Array.prototype.push;


/**
 * push list of items into array
 */
function array_push_all(array, items) {
    // see http://jsperf.com/concat-vs-push-apply/39
    // using Function.apply with items list to sends all the items
    // to the push function which actually does: array.push(items[0], items[1], ...)
    _cached_array_push.apply(array, items);
    return array;
}

function array_push_keep_latest(array, items, limit) {
    array = array || [];
    array_push_all(array, items);
    return array.length > limit ? array.slice(-limit) : array;
}

/**
 * add to array, create it in the object if doesnt exist
 */
function named_array_push(obj, arr_name, item) {
    var arr = obj[arr_name];
    if (arr) {
        _cached_array_push.call(arr, item);
    } else {
        arr = [item];
        obj[arr_name] = arr;
    }
    return arr;
}

function deep_freeze(obj) {

    // Checking isFrozen is the break condition for the recursion
    // Since isFrozen(non_object)===false it will break if it is number,string,etc,..
    // But note that any shallow frozen object will not get recursive freeze by this
    if (Object.isFrozen(obj)) return obj;

    // Cannot freeze buffers - TypeError: Cannot freeze array buffer views with elements
    if (Buffer.isBuffer(obj)) return obj;

    Object.freeze(obj);

    // Freeze all properties
    const keys = Object.keys(obj);
    for (var i = 0; i < keys.length; ++i) {
        const k = keys[i];
        const v = obj[k];
        deep_freeze(v);
    }

    return obj;
}


/**
 * Creates an object from a list of keys, intializing each key using the given value provider.
 */
function make_object(keys, valueProvider) {
    valueProvider = _.isFunction(valueProvider) ? valueProvider : _.noop;
    return _.reduce(keys, (obj, key) => {
        obj[key] = valueProvider(key);
        return obj;
    }, {});
}


function default_value(val, def_val) {
    return _.isUndefined(val) ? def_val : val;
}

/**
 * returns a compare function for array.sort(compare_func)
 * @param key_getter takes array item and returns a comparable key
 * @param order should be 1 or -1
 */
function sort_compare_by(key_getter, order) {
    key_getter = key_getter || (item => item);
    order = order || 1;
    return function(item1, item2) {
        const key1 = key_getter(item1);
        const key2 = key_getter(item2);
        if (key1 < key2) return -order;
        if (key1 > key2) return order;
        return 0;
    };
}

/**
 * Loading object properties into a new object inside a constructor
 * to allow v8 make this object as efficient as possible.
 */
class PackedObject {
    constructor(obj) {
        const keys = Object.keys(obj);
        for (var i = 0; i < keys.length; ++i) {
            this[keys[i]] = obj[keys[i]];
        }
    }
}

/**
 * Returns an empty object with lazy custom inspection
 * to avoid creating large strings when not the log print is optional.
 *
 * Example:
 * dbg.log2(inspect_lazy(very_deep_object, { depth: null, colors: true }));
 *
 */
function inspect_lazy(obj, ...inspect_args) {
    return {
        [util.inspect.custom]() {
            return util.inspect(obj, ...inspect_args);
        }
    };
}

/**
 * Create an array of size 'length' running an initializer for each item
 * to provide the item's value based on the item's position in the array.
 * If not item initializer is not provided an array where each item contain
 * the index of the item will be created.
 */
function make_array(length, item_initializer) {
    if (!_.isFunction(item_initializer)) {
        item_initializer = _.identity;
    }

    return Array.from({ length }, (_unused_, i) => item_initializer(i));
}

/**
 * Get the value indexed by a key from a map. If the key is not present in the
 * map, create a new value using the item_initializer, set it to the ket in the map
 * and return it.
 */
function map_get_or_create(map, key, item_initializer) {
    if (!_.isFunction(item_initializer)) {
        item_initializer = () => ({});
    }

    if (map.has(key)) {
        return map.get(key);
    } else {
        const val = item_initializer();
        map.set(key, val);
        return val;
    }
}

/**
 * Based upon https://github.com/lodash/lodash/blob/master/.internal/baseSortedIndexBy.js
 * @param {Array} array The sorted array to inspect.
 * @param {*} value The value to evaluate.
 * @param {Function} iteratee The iteratee invoked per element.
 * @returns {number} Returns the index at which `value` should be inserted
 *  into `array`.
 */
function findSortedIndexBy(array, value, iteratee) {
    let low = 0;
    let high = array === null ? 0 : array.length;
    if (high === 0) {
        return -1;
    }

    value = iteratee(value);

    const valIsNaN = value !== value;
    const valIsNull = value === null;
    const valIsSymbol = _.isSymbol(value);
    const valIsUndefined = value === undefined;

    while (low < high) {
        let setLow;
        const mid = Math.floor((low + high) / 2);
        const computed = iteratee(array[mid]);

        const othIsDefined = computed !== undefined;
        const othIsNull = computed === null;
        const othIsReflexive = computed === computed;
        const othIsSymbol = _.isSymbol(computed);

        if (valIsNaN) {
            setLow = othIsReflexive;
        } else if (valIsUndefined) {
            setLow = othIsReflexive && othIsDefined;
        } else if (valIsNull) {
            setLow = othIsReflexive && othIsDefined && !othIsNull;
        } else if (valIsSymbol) {
            setLow = othIsReflexive && othIsDefined && !othIsNull && !othIsSymbol;
        } else if (othIsNull || othIsSymbol) {
            setLow = false;
        } else {
            if (computed === value) return mid;
            setLow = (computed < value);
        }
        if (setLow) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return -1;
}

exports.self_bind = self_bind;
exports.array_push_all = array_push_all;
exports.array_push_keep_latest = array_push_keep_latest;
exports.named_array_push = named_array_push;
exports.deep_freeze = deep_freeze;
exports.make_object = make_object;
exports.default_value = default_value;
exports.sort_compare_by = sort_compare_by;
exports.PackedObject = PackedObject;
exports.inspect_lazy = inspect_lazy;
exports.make_array = make_array;
exports.map_get_or_create = map_get_or_create;
exports.findSortedIndexBy = findSortedIndexBy;
