/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */

var assert = require('assert-plus');
var bignum = require('bignum');
var bunyan = require('bunyan');
var crypto = require('crypto');
var util = require('util');
var sprintf = util.format;
var verror = require('verror');

/**
 * Find the hashspace a specific vnode maps to by multiplying the vnode by
 * hashspaceInterval.
 * @param {Object} options The options object.
 * @param {String} options.vnode The vnode.
 * @param {Bignum} options.vnodeHashInterval The vnode hash interval.
 *
 * @return {String} the hex representation of the beginning of the hashspace
 * the vnode maps to.
 */
function findHashspace(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.vnodeHashInterval, 'options.vnodeHashinterval');
    assert.number(options.vnode, 'options.vnode');

    var log = options.log;
    var vnodeHashInterval = options.vnodeHashInterval;
    var vnode = options.vnode;

    log.debug({
        vnode: vnode,
        interval: vnodeHashInterval
    }, 'fash.findHashspace: entering');

    var hashspace = vnodeHashInterval.mul(vnode);

    log.debug({
        vnode: vnode,
        interval: vnodeHashInterval,
        hashspace: hashspace
    }, 'ConsistentHash.findHashspace: exiting');

    return hashspace.toString(16);
};

/**
 * Simply divide the hash by the number of vnodes to find which vnode maps to
 * this hash.
 * @param {Object} options The options object.
 * @param {String} options.hash the value of the hash string in hex.
 * @param {Bignum} options.vnodeHashInterval The vnode hash interval.
 * @return {Integer} the vnode.
 */
function findVnode(options) {
    assert.object(options, 'options');
    assert.object(options.vnodeHashInterval, 'options.vnodeHashinterval');
    assert.string(options.hash, 'options.hash');
    return parseInt(bignum(options.hash, 16).div(options.vnodeHashInterval), 10);
};

/**
 * exports
 */
module.exports = {
    findHashspace: findHashspace,
    findVnode: findVnode
};
