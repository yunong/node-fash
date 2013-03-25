/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */

var assert = require('assert-plus');
var bignum = require('bignum');
var bunyan = require('bunyan');
var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var sprintf = util.format;


/**
 * Creates an instance of ConsistentHash.
 *
 * @constructor
 * @this {ConsistentHash}
 *
 * @param {Object} options The options object
 * @param [Object] options.log The optional Bunyan log object.
 * @param {String} options.algorithm The hash algorithm.
 * @param {String} options.algorithmMax The max message size of the algorithm.
 * @param {Number} options.vnodes The number of virtual nodes in the ring. This
 *                 can't be changed once set.
 * @param {String[]} options.pnodes The set of physical nodes in the ring, or
 *                                  the ring topology array.
 * @param {Object[]} topology The topology of a previous hash ring. Used to
 *                            restore an old hash ring.
 */
function ConsistentHash(options) {
    assert.object(options, 'options');
    assert.string(options.algorithm, 'options.algorithm');
    assert.string(options.algorithmMax, 'options.algorithmMax');
    assert.optionalNumber(options.vnodes, 'options.vnodes');
    assert.optionalArrayOfString(options.pnodes, 'options.pnodes');
    assert.optionalArrayOfObject(options.topology, 'options.topology');

    EventEmitter.call(this);

    this.log = options.log;

    if (!this.log) {
        this.log = bunyan.createLogger({
            name: 'fash',
            level: (process.env.LOG_LEVEL || 'warn'),
            stream: process.stderr
        });
    }
    var self = this;
    var log = self.log;

    log.trace('new ConsistentHash with options', options);

    self.algorithm = options.algorithm;

    self.algorithmMax = bignum(options.algorithmMax, 16);

    /*
     * The number of virtual nodes to provision in the ring. Once set, this
     * can't be changed.
     */
    self.vnodes = options.vnodes || 100000;
    self.vnodesBignum = bignum(self.vnodes, 10);
    self.vnodeHashInterval_ = self.algorithmMax.div(self.vnodesBignum);

    /**
     * The String array of physical nodes in the ring.
     */
    self.pnodes = options.pnodes || [];
    self.pnodes.sort();

    /**
     * Map of {pnode -> {vnode1, vnode2, ...}} Keeps track of the physical node
     * to virtual node mapping
     */
    self.pnodeToVnodeMap_ = {};

    /**
     * Map of {vnode -> pnode}
     */
    self.vnodeToPnodeMap_ = {};

    /**
     * The sorted array of [{hashspace, pnode, vnode}, ... ] that represents
     * the hash ring. This can be used to persist the topology of the hash ring
     * and instantiate a new client with the same topology.
     */
    self.ring_ = new Array(self.vnodes);

    var pnodeMap = {};

    if (options.topology) {
        var topology = options.topology;
        log.info('deserializing an already existing ring.');
        log.debug({topology: topology}, 'previous topology');

        self.vnodes = topology.length + 1;
        // clone the ring
        for (var i = 0; i < topology.length; i++) {
            var node = topology[i];
            // add to maps
            if (!self.pnodeToVnodeMap_[node.pnode]) {
                self.pnodeToVnodeMap_[node.pnode] = {};
            }
            self.pnodeToVnodeMap_[node.pnode][node.vnode] = true;
            self.vnodeToPnodeMap_[node.vnode] = node.pnode;
            if (!pnodeMap[node.pnode]) {
                pnodeMap[pnode] = true;
                self.pnodes.push(node.pnode);
            }
            // parse the hashspace, since it's serialized in a hex string
            var _hashspace = bignum(node.hashspace, 16);
            // add to the ring
            self.ring_.push({
                hashspace: node.hashspace,
                _hashspace: _hashspace,
                pnode: node.pnode,
                vnode: node.vnode
            });
        }

        self.ring_.sort(sortRing);
    } else {
        log.info('instanting new ring from scratch.');
        var pnode;
        // instantiate pnodeToVnodeMap_
        for (var l = 0; l < self.pnodes.length; l++) {
            pnode = self.pnodes[l];
            // make sure there are no duplicate keys in self.pnodes
            if (pnodeMap[pnode]) {
                throw new Error('Unable to instantiate ring, ' +
                    'duplicate pnodes in input');
            }
            pnodeMap[pnode] = true;
            self.pnodeToVnodeMap_[self.pnodes[l]] = {};
        }

        // Allocate the vnodes to the pnodes by
        // vnode % total_pnode = assigned pnode.
        for (var k = 0; k < self.vnodes; k++) {
            pnode = self.pnodes[k % self.pnodes.length];
            var vnode = k;
            var hashspace = findHashspace(self, vnode);

            log.debug({
                hashspace: hashspace,
                vnode: vnode,
                pnode: pnode
            }, 'ConsistentHash.new: assigning hashspace to vnode to pnode');

            if (!self.pnodeToVnodeMap_[pnode]) {
                self.pnodeToVnodeMap_[pnode] = {};
            }
            // assign the pnode->vnode and vnode->pnode maps
            self.pnodeToVnodeMap_[pnode][vnode] = true;
            self.vnodeToPnodeMap_[vnode] = pnode;
            self.ring_[k] = {
                hashspace: hashspace,
                vnode: vnode,
                pnode: pnode
            };

            log.debug({
                vnode: vnode,
                pnode: pnode
            }, 'ConsistentHash.new: added vnode to pnode');
        }
    }


    log.info('instantiated ring');
    log.debug({
        ring: self.ring_
    }, 'ConsistentHash.new: ring state');
}

/**
 * @exports ConsistentHash as Consistenthash
 */
module.exports = ConsistentHash;
util.inherits(ConsistentHash, EventEmitter);

/**
 * Remaps a pnode on the hash ring. The node can be an existing pnode, or a new
 * one. Emits an 'update' event with the ring topology when done.
 *
 * @param {String} node The name of the node.
 * @param {Number[]} The vnodes to add to this pnode. Implicitly removes the
 *                   vnodes from its previous pnode owner.
 * @param {function} cb The optional callback f({Error}, {Object}[], {Object}),
 *                      where {Object}[] is the updated ring topology, and
 *                      {Object} is the set of mutated pnodes as a result of
 *                      the remap.  The last two arguments are identical to
 *                      what's returned by the update event.
 * @event update
 * @param {Object[]} ring The updated ring topology.
 * @param {String} ring.hashspace The location of the node on the ring in hex.
 * @param {String} ring.node The name of the physical node.
 * @param {String} ring.vnode The vnode that the pnode maps to.
 * @param {Object} changedNodes The pnode->vnode mapping of the nodes that have
 *                              changed.
 */
ConsistentHash.prototype.remapVnode = function remapVnode(pnode, vnodes, cb) {
    var self = this;
    var log = self.log;
    log.info({
        newNode: pnode,
        vnodes: vnodes
    }, 'ConsistentHash.remapVnode: entering');
    assert.string(pnode, 'pnode');

    var newPnode = pnode;
    self.pnodeToVnodeMap_[newPnode] = {};

    // keeps track of which pnodes have changed {pnode->vnode}
    var changedNodes = {};

    // remove vnodes from old and add to new pnode
    for (var i = 0; i < vnodes.length; i++) {
        var vnode = parseInt(vnodes[i], 10);
        var oldPnode = self.vnodeToPnodeMap_[vnode];
        log.info({
            vnode: vnode,
            oldPnode: oldPnode,
            pnode: pnode
        }, 'ConsistentHash.remapVnode: remopping vnode');
        // remove vnode from current pnode mapping.
        delete self.pnodeToVnodeMap_[oldPnode][vnode];

        // add vnode to new pnode
        self.pnodeToVnodeMap_[newPnode][vnode] = true;
        self.vnodeToPnodeMap_[vnode] = newPnode;

        // update the ring
        var hashspace = findHashspace(self, vnode);
        var ringIdx = self.findVnode(hashspace);
        var ringElement = self.ring_[ringIdx];
        var newRingElement = {
            hashspace: hashspace,
            vnode: vnode,
            pnode: newPnode
        };

        log.info({
            ringElement: ringElement,
            ringIndex: ringIdx,
            newRingElement: newRingElement
        }, 'ConsistentHash.remapVnode: replacing ring element');
        // check the element in the ring contains the right hashspace
        if (hashspace !== ringElement.hashspace) {
            var errMsg = 'ConsistentHash.remapVnode: internal error, ' +
                'hashspace does not correspond to ring state';
            log.fatal({
                ringIndex: ringIdx,
                hashspace: hashspace,
                ringElement: ringElement
            }, errMsg);

            var err = new Error(errMsg);
            self.emit('error', err);
            if (cb) {
                return cb(err);
            }
        }

        self.ring_[ringIdx] = newRingElement;

        // update which pnodes have changed
        if (!changedNodes[oldPnode]) {
            changedNodes[oldPnode] = [];
        }
        changedNodes[oldPnode].push(vnode);
    }

    // add pnode to pnodeList
    self.pnodes.push(pnode);
    self.pnodes.sort();

    log.trace({
        ring: self.ring_
    }, 'ConsistentHash.remapVnode: updatedRing', self.ring_);

    log.info({
        newNode: pnode,
        vnodes: vnodes
    }, 'ConsistentHash.remapVnode: exiting');

    self.emit('update', self.ring_, changedNodes);
    if (cb) {
        return cb(null, self.ring_, changedNodes);
    }

    return (undefined);
};

/**
 * Removes a pnode from the hash ring.  Note the pnode must not map to any
 * vnodes.  Remove the vnodes first by re-assigning them to other pnodes before
 * invoking this function.
 *
 * @param {String} pnode The pnode to remove.
 * @param {function} cb The optional callback f({Error}, {Object}[], {String})
 *                      The last two arguments are identical to what's returned
 *                      by the remove event.
 * @event update
 * @param {Object[]} ring The updated ring topology.
 * @param {String} ring.hashspace The location of the node on the ring in hex.
 * @param {String} ring.node The name of the physical node.
 * @param {String} ring.vnode The vnode that the pnode maps to.
 * @param {Object} changedNodes The pnode->vnode mapping of the nodes that have
 *                              changed.
 */
ConsistentHash.prototype.removePnode = function removePnode(pnode, cb) {
    var self = this;
    var log = self.log;

    log.info({
        pnode: pnode
    }, 'ConsistentHash.removePnode: entering');

    assert.string(pnode, 'pnode');
    // check that the pnode exists
    var vnodes = self.pnodeToVnodeMap_[pnode];
    if (!vnodes) {
        log.warn('pnode is not in ring', pnode);
        return cb();
    }

    if (Object.keys(vnodes).length > 0) {
        var errMsg = 'pnode still maps to vnodes, re-assign vnodes first';
        log.error(errMsg);
        var err = new Error(errMsg);
        self.emit('error', err);
        if (cb) {
            return cb(err);
        }
    }

    // remove references to pnode.
    self.pnodes.splice(self.pnodes.indexOf(pnode));
    delete self.pnodeToVnodeMap_[pnode];

    log.info({
        pnode: pnode
    }, 'ConsistentHash.removePnode: exiting');

    var changedMapping = {};
    changedMapping[pnode] = [];
    self.emit('update', self.ring_, changedMapping);
    if (cb) {
        return cb(null, self.ring_, pnode);
    }

    return (undefined);
};

/**
 * Get the array of vnodes that belong to a particular pnode
 *
 * @param {String} pnode The pnode.
 * @return {Object} vnodes The map of vnodes.
 */
ConsistentHash.prototype.getVnodes = function getVnodes(pnode) {
    var self = this;
    var log = self.log;

    log.info({pnode: pnode}, 'ConsistentHash.getVnodes: engering');
    assert.string(pnode, 'pnode');
    // check that the pnode exists
    var vnodes = self.pnodeToVnodeMap_[pnode];
    if (!vnodes) {
        log.warn('pnode is not in ring', pnode);
        throw new Error('pnode is not in ring', pnode);
    }

    var vnodeArray = [];
    for (var vnode in vnodes) {
        vnodeArray.push(vnode);
    }
    log.info({
        pnode: pnode,
        vnodes: vnodes,
        vnodeArray: vnodeArray
    }, 'ConsistentHash.getVnodes: exiting');
    return vnodeArray;
};

/**
 * Gets the node that a key belongs to on the ring.
 *
 * @param {String} key The key.
 *
 * @returns {pnode, vnode} node The pnode and vnode that the key maps to.
 */
ConsistentHash.prototype.getNode = function getNode(key) {
    var self = this;
    var log = self.log;

    var hash = crypto.createHash(self.algorithm);
    hash.update(key);
    var value = hash.digest('hex');
    log.debug({
        key: key,
        value: value
    }, 'ConsistentHash.getNode: hashing key to value');

    // find the node that corresponds to this hash.
    var ringIndex = self.findVnode(value);
    log.trace('key %s hashes to node', key, self.ring_[ringIndex]);
    return {
        pnode: self.ring_[ringIndex].pnode,
        vnode: self.ring_[ringIndex].vnode
    };
};

/**
 * Serialize the current state of the ring.
 *
 * @return {Object[]} ring The updated ring topology.
 * @return {String} ring.hashspace The location of the node on the ring in hex.
 * @return {String} ring.node The name of the physical node.
 * @return {String} ring.vnode The vnode that the pnode maps to.
 */
ConsistentHash.prototype.serialize = function serialize() {
    var self = this;
    var log = self.log;
    log.info('ConsistentHash.serialize: entering');
    // deep copy the ring
    var ring = new Array(self.vnodes);
    for (var i = 0; i < self.vnodes; i++) {
        var hashspace = self.ring_[i];
        var copiedHashspace = {
            hashspace: new String(hashspace.hashspace),
            pnode: new String(hashspace.pnode),
            vnode: parseInt(hashspace.vnode, 10)
        };
        log.trace({
            original: hashspace,
            copied: copiedHashspace
        }, 'ConsistentHash.serialize: deep copying entry');
        ring[i] = copiedHashspace;
    }
    log.trace({
        ring: ring
    }, 'ConsistentHash.serialize: fully serialized ring');
    log.info('ConsistentHash.serialize: exiting');
    return ring;
};

// Private Functions

/**
 * Simply divide the hash by the number of vnodes to find which vnode maps to
 * this hash.
 * @param {String} the value of the hash string in hex.
 * @return {Integer} the vnode.
 */
ConsistentHash.prototype.findVnode = function(hash) {
    var self = this;
    var log = self.log;
    log.debug({
        hash: hash.toString(),
        max: self.algorithmMax.toString(),
        interval: self.vnodeHashInterval_
    }, 'ConsistentHash.findVnode: entering findVnode');

    var hashBignum = bignum(hash, 16);
    var vnodeIdx = hashBignum.div(self.vnodeHashInterval_);

    log.debug({
        hash: hash.toString(),
        vnodIdx: vnodeIdx.toString()
    }, 'ConsistentHash.findVnode: got vnode index');

    return parseInt(vnodeIdx, 10);
}

/**
 * find the hashspace a specific vnode maps to. multiply vnode by
 * self.hashspaceInterval.
 * @param {Integer} the vnode.
 * @return {String} the hex representation of the beginning of the hashspace
 * the vnode maps to.
 */
var findHashspace = function(self, vnode) {
    var log = self.log;
    log.debug({
        vnode: vnode,
        interval: self.vnodeHashInterval_
    }, 'ConsistentHash.findHashspce: entering');

    var hashspace = self.vnodeHashInterval_.mul(vnode);

    log.debug({
        vnode: vnode,
        interval: self.vnodeHashInterval_,
        hashspace: hashspace
    }, 'ConsistentHash.findHashspce: exiting');

    return hashspace.toString(16);
};

/**
 * sorts the hashring from the smallest integer to the largest.
 */
function sortRing(a, b) {
    a = a._hashspace;
    b = b._hashspace;
    if (a.lt(b)) {
        return -1;
    }
    if (a.gt(b)) {
        return 1;
    }
    return 0;
}

function sort(a, b) {
    a = bignum(a, 16);
    b = bignum(b, 16);
    if (a.lt(b)) {
        return -1;
    }
    if (a.gt(b)) {
        return 1;
    }
    return 0;
}
