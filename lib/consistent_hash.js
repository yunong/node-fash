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
    self.algorithmMax_ = bignum(options.algorithm.MAX, 16);

    /*
     * The number of virtual nodes to provision in the ring. Once set, this
     * can't be changed.
     */
    self.vnodeCount_ = options.vnodes || options.topology.vnodes || 100000;
    self.vnodesBignum_ = bignum(self.vnodeCount_, 10);
    self.vnodeHashInterval_ = self.algorithmMax_.div(self.vnodesBignum_);

    /**
     * The String array of physical nodes in the ring.
     */
    self.pnodes_ = options.pnodes ? options.pnodes.slice() : [];
    self.pnodes_.sort();

    /**
     * Map of {pnode -> {vnode1 ->{}, vnode2, ...}} Keeps track of the physical
     * node to virtual node mapping. Also keeps track of an optional data
     * object.
     */
    self.pnodeToVnodeMap_ = {};

    /**
     * Map of {vnode -> {pnode, data}. Contains the pnode and an optional data
     * object. If you want the actual pnode, you'll need to dereference as
     * self.vnodeToPnodeMap_[vnode].pnode -- otherwise you'll get the object.
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
            self.pnodes_.push(pnode);
            var vnodes = self.pnodeToVnodeMap_[pnode];
            Object.keys(vnodes).forEach(function(vnode) {
                self.vnodeToPnodeMap_[vnode] = {
                    pnode: pnode,
                    data: vnodes[vnode]
                };
            });
        });

        self.pnodes_.sort();

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
                throw new verror.VError('Unable to instantiate ring, ' +
                    'duplicate pnodes in input');
            }
            pnodeMap[pnode] = true;
            self.pnodeToVnodeMap_[self.pnodes_[index]] = {};
        });

        // Allocate the vnodes to the pnodes by
        // vnode % total_pnode = assigned pnode.
        function allocateVnode() {
            for (var vnode = 0; vnode < self.vnodeCount_; vnode++) {
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
                // set the data here to null since this is a new ring
                self.pnodeToVnodeMap_[pnode][vnode] = null;
                self.vnodeToPnodeMap_[vnode] = {
                    pnode: pnode
                    // don't set data here -- since this is a new ring
                };

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
module.exports = {
    ConsistentHash: ConsistentHash,
    VERSION: '1.2.0'
};

/**
 * Adds a data element to a vnode. If data already existed for the vnode, this
 * will over-write it. This is useful if you want to add stateful changes to a
 * set of particular vnodes -- for example, setting some vnodes to read only.
 *
 * @param {Number} vnode The vnode to add data to.
 * @param {Object} data The data to add to the vnode.
 * @param {function} cb The optional callback f({Object}, {Object}).
 * @param {Object} cb.newTopology The updated ring topology.
 * @param {Object} cb.newTopology.pnodeToVnodeMap The map of physical nodes to.
 * @param {String} cb.newTopology.vnode The number of vnodes in the ring.
 */
ConsistentHash.prototype.addData = function addData(vnode, data, cb) {
    var self = this;
    var log = self.log;
    log.info({
        vnodes: vnode,
        data: data
    }, 'ConsistentHash.addData: entering');
    assert.number(vnode, 'vnode');
    var pnode = self.vnodeToPnodeMap_[vnode];
    assert.object(pnode, 'vnode ' + vnode + ' doesn\'t map to pnode');

    // data needs to be changed in both pvmap and vpmap
    // change pvmap
    pnode.data = data;
    // change vpmap
    self.pnodeToVnodeMap_[pnode.pnode][vnode] = data;

    if (cb) {
        var newTopology = JSON.parse(self.serialize());
        return cb(newTopology);
    }

    log.info({
        vnode: vnode,
        data: data
    }, 'ConsistentHash.addData: exiting');

    return (undefined);
};

/**
 * Remaps a pnode on the hash ring. The node can be an existing pnode, or a new
 * one.
 *
 * @param {String} node The name of the node.
 * @param {Number[]} The vnodes to add to this pnode. Implicitly removes the
 *                   vnodes from its previous pnode owner.
 * @param {function} cb The optional callback f({Object}, {Object}).
 * @param {Object} cb.newTopology The updated ring topology.
 * @param {Object} cb.newTopology.pnodeToVnodeMap The map of physical nodes to.
 * @param {String} cb.newTopology.vnode The number of vnodes in the ring.
 * @param {Object} cb.changedNodes The pnode->vnode mapping of the nodes that
 *                                 have changed.
 */
ConsistentHash.prototype.remapVnode = function remapVnode(newPnode, vnodes, cb) {
    var self = this;
    var log = self.log;
    log.info({
        newNode: newPnode,
        vnodes: vnodes
    }, 'ConsistentHash.remapVnode: entering');
    assert.string(newPnode, 'newPnode');
    assert.optionalArrayOfNumber(vnodes, 'vnodes');

    // assert the vnodes, ensuring that:
    // 1) vnode actually exist.
    // 2) vnode doesn't already belong to the newPnode.
    // 3) vnodes are specified once and only once.
    var vnodeMap = {};
    if (vnodes) {
        vnodes.forEach(function(v) {
            if ((v > self.vnodeCount_) || (v < 0)) {
                throw new verror.VError('vnode ' + v +
                                        ' does not exist in the ring');
            }
            if (vnodeMap[v]) {
                throw new verror.VError('vnode ' + v +
                                        ' specified more than once');
            }

            vnodeMap[v] = true;
            // check that the vnode doesn't already belong to the newPnode.
            if (self.vnodeToPnodeMap_[v].pnode === newPnode) {
                throw new verror.VError('vnode ' + v +
                                        ' already belongs to pnode');
            }
        });
    }

    // if this pnode doesn't exist, create it
    if (!self.pnodeToVnodeMap_[newPnode]) {
        self.pnodeToVnodeMap_[newPnode] = {};
        self.pnodes_.push(newPnode);
        self.pnodes_.sort();
    }

    // keeps track of which pnodes have changed {pnode->vnode}
    var changedNodes = {};

    // remove vnodes from the old pnode and add to new pnode
    for (var i = 0; i < vnodes.length; i++) {
        var vnode = parseInt(vnodes[i], 10);
        var oldPnode = self.vnodeToPnodeMap_[vnode].pnode;
        var vnodeData = self.vnodeToPnodeMap_[vnode].data;
        log.info({
            vnode: vnode,
            oldPnode: oldPnode,
            newPnode: newPnode,
            vnodeData: vnodeData
        }, 'ConsistentHash.remapVnode: remopping vnode');

        // add vnode to new pnode
        // 1) move the vnode object from the old pvmap to the new pvmap. Since
        // we're just moving the vnode, there's no need to write any new values
        self.pnodeToVnodeMap_[newPnode][vnode] =
            self.pnodeToVnodeMap_[oldPnode][vnode];
        // 2) add a new pnode,data object to the vpmap for the current vnode.
        self.vnodeToPnodeMap_[vnode] = {
            pnode: newPnode,
            data: vnodeData
        };

        // remove vnode from current pnode mapping. but first set the value to
        // null -- otherwise the vnode gets removed from the new pnode mappings
        // as well.
        self.pnodeToVnodeMap_[oldPnode][vnode] = null;
        delete self.pnodeToVnodeMap_[oldPnode][vnode];

        // update which pnodes have changed
        if (!changedNodes[oldPnode]) {
            changedNodes[oldPnode] = [];
        }
        changedNodes[oldPnode].push(vnode);
    }

    log.trace({
        pnodeToVnodeMap: self.pnodeToVnodeMap_,
        vnodeToPnodeMap: self.vnodeToPnodeMap_
    }, 'ConsistentHash.remapVnode: updated');

    log.info({
        newNode: newPnode,
        vnodes: vnodes
    }, 'ConsistentHash.remapVnode: exiting');

    if (cb) {
        var newTopology = JSON.parse(self.serialize());
        return cb(newTopology, changedNodes);
    }

    return (undefined);
};

/**
 * Removes a pnode from the hash ring.  Note the pnode must not map to any
 * vnodes.  Remove the vnodes first by re-assigning them to other pnodes before
 * invoking this function.
 *
 * @param {String} pnode The pnode to remove.
 * @param {function} cb The optional callback f({Object}, {Object}).
 * @param {Object} cb.newTopology The updated ring topology.
 * @param {Object} cb.newTopology.pnodeToVnodeMap The map of physical nodes to.
 * @param {String} cb.newTopology.vnode The number of vnodes in the ring.
 * @param {Object} cb.changedNodes The pnode->vnode mapping of the nodes that have
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
        var msg = sprintf('pnode %s not in ring, skipping', pnode);
        throw new verror.VError(msg);
    }

    if (Object.keys(vnodes).length > 0) {
        var errMsg = 'pnode still maps to vnodes, re-assign vnodes first';
        throw new new verror.VError(errMsg);
    }

    // remove references to pnode.
    self.pnodes_.splice(self.pnodes_.indexOf(pnode), 1);
    self.pnodeToVnodeMap_[pnode] = null;
    delete self.pnodeToVnodeMap_[pnode];

    log.info({
        pnode: pnode
    }, 'ConsistentHash.removePnode: exiting');

    if (cb) {
        var newTopology = JSON.parse(self.serialize());
        return cb(newTopology, pnode);
    }

    return (undefined);
};

/**
 * Get the array of vnodes that belong to a particular pnode
 *
 * @param {String} pnode The pnode.
 * @return {Arrary} vnodes The numeric array of vnodes.
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
        throw new verror.VError('pnode is not in ring', pnode);
    }

    var vnodeArray = [];
    Object.keys(vnodes).forEach(function(vnode) {
        vnodeArray.push(parseInt(vnode, 10));
    });
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
    var value = crypto.createHash(this.algorithm_.NAME).update(key).digest('hex');
    // find the node that corresponds to this hash.
    var vnode = this.findVnode(value).toString();
    var pnode = this.vnodeToPnodeMap_[vnode].pnode;
    var data = this.pnodeToVnodeMap_[pnode][vnode];
    return {pnode: pnode, vnode: vnode, data: data};
};

/**
 * Serialize the current state of the ring in serialized to a JSON string.
 *
 * @return {Object} ring The updated ring topology.
 * @return {Object} ring.pnodeToVnodeMap The map of {pnode->{vnode1,... vnoden}.
 * @return {String} ring.vnode The number of vnodes in the ring.
 */
ConsistentHash.prototype.serialize = function serialize() {
    var self = this;
    var log = self.log;
    log.info('ConsistentHash.serialize: entering');
    // deep copy the ring
    var serializedHash = JSON.stringify({
        vnodes:  self.vnodeCount_,
        pnodeToVnodeMap: self.pnodeToVnodeMap_,
        algorithm: self.algorithm_.NAME,
        version: module.exports.VERSION
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
    return parseInt(bignum(hash, 16).div(this.vnodeHashInterval_), 10);
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
