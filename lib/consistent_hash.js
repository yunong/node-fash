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
 * @param {Object} options.algorithm The hash algorithm object.
 * @param {String} options.algorithm.algorithm The hash algorithm.
 * @param {String} options.algorithm.max The max output size of the algorithm.
 * @param {Number} options.vnodes The number of virtual nodes in the ring. This
 *                 can't be changed once set.
 * @param {String[]} options.pnodes The optional array of physical nodes in the
 *                   ring, or the ring topology array.
 * @param {Object} topology The topology of a previous hash ring. Used to
 *                 restore an old hash ring.
 * @param {Object} topology.pnodeToVnodeMap The mapping of pnode to vnodes of
 *                 the serialized topology.
 * @param {Number} topology.vnodes The number of vnodes in the serialized
 *                 topology.
 */
function ConsistentHash(options) {
    assert.object(options, 'options');

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

    /**
     * The hash algorithm used determine the position of a key.
     */
    self.algorithm_ = options.algorithm;

    /**
     * The maximum output size of the hash algorithm. Used to determine the
     * hash interval between each vnode.
     */
    self.algorithmMax_ = bignum(options.algorithm.max, 16);

    /*
     * The number of virtual nodes to provision in the ring. Once set, this
     * can't be changed.
     */
    self.vnodes_ = options.vnodes || options.topology.vnodes || 100000;
    self.vnodesBignum_ = bignum(self.vnodes_, 10);
    self.vnodeHashInterval_ = self.algorithmMax_.div(self.vnodesBignum_);

    /**
     * The String array of physical nodes in the ring.
     */
    self.pnodes_ = options.pnodes ? options.pnodes.slice() : [];
    self.pnodes_.sort();

    /**
     * Map of {pnode -> {vnode1, vnode2, ...}} Keeps track of the physical node
     * to virtual node mapping
     */
    self.pnodeToVnodeMap_ = {};

    /**
     * Map of {vnode -> pnode}
     */
    self.vnodeToPnodeMap_ = {};

    var pnodeMap = {};

    if (options.topology) {
        var topology = options.topology;
        log.info('ConsistentHash.new: deserializing an already existing ring.');
        log.debug({
            topology: topology
        }, 'ConsistentHash.new: previous topology');
        self.pnodeToVnodeMap_ = topology.pnodeToVnodeMap;
        var pnodeKeys = Object.keys(self.pnodeToVnodeMap_);

        pnodeKeys.forEach(function(pnode) {
            var vnodes = self.pnodeToVnodeMap_[pnode];
            Object.keys(vnodes).forEach(function(vnode) {
                self.vnodeToPnodeMap_[vnode] = pnode;
            });
        });

        log.info('ConsistentHash.new: finished deserializing');
        log.debug({
            pnodeToVnodeMap: self.pnodeToVnodeMap_,
            vnodeToPnodeMap: self.vnodeToPnodeMap_
        }, 'ConsistentHash.new: topology');
    } else {
        log.info('instanting new ring from scratch.');
        // instantiate pnodeToVnodeMap_
        self.pnodes_.forEach(function(pnode, index) {
            // make sure there are no duplicate keys in self.pnodes_
            if (pnodeMap[pnode]) {
                throw new Error('Unable to instantiate ring, ' +
                    'duplicate pnodes in input');
            }
            pnodeMap[pnode] = true;
            self.pnodeToVnodeMap_[self.pnodes_[index]] = {};
        });

        // Allocate the vnodes to the pnodes by
        // vnode % total_pnode = assigned pnode.
        function allocateVnode() {
            for (var vnode = 0; vnode < self.vnodes_; vnode++) {
                var pnode = self.pnodes_[vnode % self.pnodes_.length];
                var hashspace = self.findHashspace(vnode);

                log.debug({
                    hashspace: hashspace,
                    vnode: vnode,
                    pnode: pnode
                }, 'ConsistentHash.new: assigning hashspace to vnode to pnode');

                if (!self.pnodeToVnodeMap_[pnode]) {
                    self.pnodeToVnodeMap_[pnode] = {};
                }
                // assign the pnode->vnode and vnode->pnode maps
                self.pnodeToVnodeMap_[pnode][vnode] = 1;
                self.vnodeToPnodeMap_[vnode] = pnode;

                log.debug({
                    vnode: vnode,
                    pnode: pnode
                }, 'ConsistentHash.new: added vnode to pnode');
            }
        }
        allocateVnode();
    }


    log.info('instantiated ring');
    log.debug({
        pnodeToVnodeMap: self.pnodeToVnodeMap_,
        vnodeToPnodeMap: self.vnodeToPnodeMap_
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
 * @param {function} cb The optional callback f({Error}, {Object}, {Object}).
 *                      The last two arguments are identical to what's returned
 *                      by the update event.
 * @event update
 * @param {Object} newTopology The updated ring topology.
 * @param {Object} newTopology.pnodeToVnodeMap The map of physical nodes to.
 * @param {String} newTopology.vnode The number of vnodes in the ring.
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

        // update which pnodes have changed
        if (!changedNodes[oldPnode]) {
            changedNodes[oldPnode] = [];
        }
        changedNodes[oldPnode].push(vnode);
    }

    // add pnode to pnodeList
    self.pnodes_.push(pnode);
    self.pnodes_.sort();

    log.trace({
        pnodeToVnodeMap: self.pnodeToVnodeMap_,
        vnodeToPnodeMap: self.vnodeToPnodeMap_
    }, 'ConsistentHash.remapVnode: updated');

    log.info({
        newNode: pnode,
        vnodes: vnodes
    }, 'ConsistentHash.remapVnode: exiting');

    var newTopology = JSON.parse(self.serialize());
    self.emit('update', newTopology, changedNodes);
    if (cb) {
        return cb(null, newTopology, changedNodes);
    }

    return (undefined);
};

/**
 * Removes a pnode from the hash ring.  Note the pnode must not map to any
 * vnodes.  Remove the vnodes first by re-assigning them to other pnodes before
 * invoking this function.
 *
 * @param {String} pnode The pnode to remove.
 * @param {function} cb The optional callback f({Error}, {Object}, {Object}).
 *                      The last two arguments are identical to what's returned
 *                      by the update event.
 * @event update
 * @param {Object} newTopology The updated ring topology.
 * @param {Object} newTopology.pnodeToVnodeMap The map of physical nodes to.
 * @param {String} newTopology.vnode The number of vnodes in the ring.
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
    self.pnodes_.splice(self.pnodes_.indexOf(pnode));
    delete self.pnodeToVnodeMap_[pnode];

    log.info({
        pnode: pnode
    }, 'ConsistentHash.removePnode: exiting');

    var changedMapping = {};
    changedMapping[pnode] = [];
    var newTopology = JSON.parse(self.serialize());
    self.emit('update', newTopology, changedMapping);
    if (cb) {
        return cb(null, newTopology, pnode);
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
 * Gets the pnode and vnode that a key belongs to on the ring.
 *
 * @param {String} key The key.
 *
 * @returns {pnode, vnode} node The pnode and vnode that the key maps to.
 */
ConsistentHash.prototype.getNode = function getNode(key) {
    var self = this;
    var log = self.log;

    var hash = crypto.createHash(self.algorithm_.algorithm);
    hash.update(key);
    var value = hash.digest('hex');
    log.debug({
        key: key,
        value: value
    }, 'ConsistentHash.getNode: hashing key to value');

    // find the node that corresponds to this hash.
    var ringIndex = self.findVnode(value);
    log.trace('key %s hashes to node', key, ringIndex);
    return {
        pnode: self.vnodeToPnodeMap_[ringIndex.toString()],
        vnode: ringIndex.toString()
    };
};

/**
 * Serialize the current state of the ring in serialized to a JSON string.
 *
 * @return {Object} ring The updated ring topology.
 * @return {Object} ring.pnodeToVnodeMap The map of physical nodes to.
 * @return {String} ring.vnode The number of vnodes in the ring.
 */
ConsistentHash.prototype.serialize = function serialize() {
    var self = this;
    var log = self.log;
    log.info('ConsistentHash.serialize: entering');
    // deep copy the ring
    var serializedHash = JSON.stringify({
        vnodes:  self.vnodes_,
        pnodeToVnodeMap: self.pnodeToVnodeMap_,
        algorithm: self.algorithm_
    });

    log.trace({
        serializedRing: serializedHash
    }, 'ConsistentHash.serialize: fully serialized ring');
    log.info('ConsistentHash.serialize: exiting');
    return serializedHash;
};

// Private Functions

/**
 * Simply divide the hash by the number of vnodes to find which vnode maps to
 * this hash.
 * @param {String} the value of the hash string in hex.
 * @return {Integer} the vnode.
 */
ConsistentHash.prototype.findVnode = function findVnode(hash) {
    var self = this;
    var log = self.log;
    log.debug({
        hash: hash.toString(),
        max: self.algorithm_.max.toString(),
        interval: self.vnodeHashInterval_
    }, 'ConsistentHash.findVnode: entering findVnode');

    var hashBignum = bignum(hash, 16);
    var vnodeIdx = hashBignum.div(self.vnodeHashInterval_);

    log.debug({
        hash: hash.toString(),
        vnodIdx: vnodeIdx.toString()
    }, 'ConsistentHash.findVnode: got vnode index');

    return parseInt(vnodeIdx, 10);
};

/**
 * find the hashspace a specific vnode maps to. multiply vnode by
 * self.hashspaceInterval.
 * @param {Integer} the vnode.
 * @return {String} the hex representation of the beginning of the hashspace
 * the vnode maps to.
 */
ConsistentHash.prototype.findHashspace = function findHashspace(vnode) {
    var self = this;
    var log = self.log;
    log.debug({
        vnode: vnode,
        interval: self.vnodeHashInterval_
    }, 'ConsistentHash.findHashspace: entering');

    var hashspace = self.vnodeHashInterval_.mul(vnode);

    log.debug({
        vnode: vnode,
        interval: self.vnodeHashInterval_,
        hashspace: hashspace
    }, 'ConsistentHash.findHashspace: exiting');

    return hashspace.toString(16);
};
