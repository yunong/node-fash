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

var assertArray = assert.assertArray;
var assertFunction = assert.assertFunction;
var assertNumber = assert.assertNumber;
var assertObject = assert.assertObject;
var assertString = assert.assertString;

/**
 * Creates an instance of ConsistentHash.
 *
 * @constructor
 * @this {ConsistentHash}
 *
 * @param {Object} options The options object
 * @param [Object] options.log The optional Bunyan log object.
 * @param {String} options.algorithm The hash algorithm.
 * @param {Number} options.numberOfReplicas The number of replicas to create for
 *                 each node.
 * @param {String[]|Object[]} options.nodes The set of physical nodes in the
 *                            ring, or the ring topology array. The String is
 *                            used to boostrap a new hashring, whilst the Object
 *                            is used to instantiate a already provisioned hash
 *                            ring.
 * @param {String} options.nodes[].node The name of the physical node.
 * @param {String} options.nodes[].replica The replica name of the node.
 * @param {String} hashspace The position on the ring the node occupies in hex
 *                 format.
 */
function ConsistentHash(options) {
        assert.object(options, 'options');
        assert.string(options.algorithm, 'options.algorithm');
        assert.optionalNumber(options.vnodes, 'options.vnodes');
        assert.optionalArrayOfString(options.pnodes, 'options.pnodes');
        assert.optionalObject(options.mapping, 'options.mapping');
        assert.optionalBool(options.random, 'options.random');
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
        self.vnodes = options.vnodes || 100000;
        self.pnodes = options.pnodes || [];
        self.random = options.random || false;
        /**
         * Map of {vnode -> hashspace} Keeps track of the hashspace that each
         * virtual node owns. Used for quick lookups on add/remove node
         */
        self.vnodeToHashspaceMap_ = {};

        /**
         * The map of {hashspace -> vnode, ...}, used for quick lookups
         * for collision detection and node removal.
         */
        self.hashspaceToVnodeMap_ = {};

        /**
         * Map of {pnode -> {vnode1, vnode2, ...}} Keeps track of the
         * physical node to virtual node mapping
         */
        self.pnodeToVnodeMap_ = {};

        /**
         * Map of {vnode -> pnode}
         */
        self.vnodeToPnodeMap_ = {};

        /**
         * The sorted array of [{hashspace, pnode, vnode}, ... ] that
         * represents the hash ring. This can be used to persist the topology
         * of the hash ring and instantiate a new client with the same topology.
         */
        self.ring = [];

        if (options.topology) {
                var topology = options.topology;
                log.info('deserializing an already existing ring.');
                log.debug('previous topology', {
                        topology: topology
                });
                self.vnodes = topology.length + 1;
                var pnodeMap = {};
                // clone the ring
                for (var i = 0; i < topology.length; i++) {
                        var node = topology[i];
                        // add to maps
                        self.vnodeToHashspaceMap_[node.vnode] = node.hashspace;
                        self.hashspaceToVnodeMap_[node.hashspace] = node.vnode;
                        if (!self.pnodeToVnodeMap_[node.pnode]) {
                                self.pnodeToVnodeMap_[node.pnode] = {};
                        }
                        self.pnodeToVnodeMap_[node.pnode][node.vnode] = true;
                        self.vnodeToPnodeMap_[node.vnode] = node.pnode;
                        if (!pnodeMap[node.pnode]) {
                                pnodeMap[pnode] = true;
                                self.pnodes.push(node.pnode);
                        }
                        // parse the hashspace, since it's serialized in a hex
                        // string
                        var _hashspace = bignum(node.hashspace, 16);
                        // add to the ring
                        self.ring.push({
                                hashspace: node.hashspace,
                                _hashspace: _hashspace,
                                pnode: node.pnode,
                                vnode: node.vnode
                        });
                }

                self.ring.sort(sortRing);
        } else {
                log.info('instanting new ring from scratch.');
                // instantiate pnodeToVnodeMap_
                var pnodeMap = {};
                for (var i = 0; i < self.pnodes.length; i++) {
                        var pnode = self.pnodes[i];
                        // make sure there's no duplicate keys in self.pnodes
                        if (pnodeMap[pnode]) {
                                throw new Error('Unable to instantiate ring, ' +
                                                'duplicate pnodes in input');
                        }
                        pnodeMap[pnode] = true;
                        self.pnodeToVnodeMap_[self.pnodes[i]] = {};
                }

                // Provision a new ring with a set of vnodes.
                for (var i = 0; i < self.vnodes; i++) {
                        // this also populates vnodeToHashspaceMap_ and
                        // hashspaceToVnodeMap_
                        addVnode(self, i);
                }

                // Allocate the vnodes to the pnodes.
                for (var i = 0; i < self.vnodes; i++) {
                        var pnode = self.pnodes[i % self.pnodes.length];
                        var vnode = i;
                        var hashspace = self.vnodeToHashspaceMap_[vnode];

                        log.debug('adding hashspace %s vnode %s to pnode %s',
                                  hashspace, vnode, pnode);
                        if (!self.pnodeToVnodeMap_[pnode]) {
                                self.pnodeToVnodeMap_[pnode] = {};
                        }
                        self.pnodeToVnodeMap_[pnode][vnode] = true;
                        self.vnodeToPnodeMap_[vnode] = pnode;
                        self.ring.push({
                                hashspace: hashspace,
                                _hashspace: bignum(hashspace, 16),
                                vnode: vnode,
                                pnode: pnode
                        });
                        log.debug('added vnode %s to pnode %s', vnode, pnode);
                }

                self.ring.sort(sortRing);
        }

        self.pnodes.sort();

        log.info('instantiated ring');
        log.debug('ring state', self.ring);
}

/**
 * @exports ConsistentHash as Consistenthash
 */
module.exports = ConsistentHash;
util.inherits(ConsistentHash, EventEmitter);

/**
 * Adds a node to the hash ring. Emits an 'update' event with the ring topology
 * when done.
 *
 * @param {String} node The name of the node.
 * @param [Number] The existing vnodes this pnode maps to.
 * @event update
 * @param {Object[]} ring The updated ring topology.
 * @param {String} ring.hashspace The location of the node on the ring in hex.
 * @param {String} ring.node The name of the physical node.
 * @param {String} ring.replica The replica name of the node.
 */
ConsistentHash.prototype.addNode = function addNode(node, vnodes) {
        var self = this;
        var log = self.log;
        log.info('entering addNode with', {
                newNode: node,
                vnodes: vnodes
        });
        assert.string(node, 'node');

        self.pnodeToVnodeMap_[node] = {};

        // keeps track of which pnodes have changed {pnode->vnode}
        var changedNodes = {};

        // remove vnodes from old and add to new pnode
        for (var i = 0; i < vnodes.length; i++) {
                var vnode = parseInt(vnodes[i], 10);
                var oldPnode = self.vnodeToPnodeMap_[vnode];
                var newPnode = node;
                // remove vnode from current pnode mapping.
                delete self.pnodeToVnodeMap_[oldPnode][vnode];
                // add vnode to new pnode
                self.pnodeToVnodeMap_[newPnode][vnode] = true;
                self.vnodeToPnodeMap_[vnode] = node;
                var hashspace = self.vnodeToHashspaceMap_[vnode];
                var ringIdx = self.binarySearch(self.ring,
                                                bignum(hashspace, 16));
                var ringElement = self.ring[ringIdx];
                var newRingElement = {
                        hashspace: hashspace,
                        _hashspace: bignum(hashspace, 16),
                        vnode: vnode,
                        pnode: newPnode
                };

                log.info('replacing ring element', {
                        ringElement: ringElement,
                        ringIndex: ringIdx,
                        newRingElement: newRingElement
                });
                // check the element in the ring contains the right hashspace
                if (hashspace !== ringElement.hashspace) {
                        var errMsg = 'internal error, hashspace does not '+
                                'correspond to ring state';
                        log.fatal(errMsg,{
                                hashspace: hashspace,
                                ringElement: ringElement
                        });

                        self.emit('error', errms);
                }

                self.ring[ringIdx] = newRingElement;

                // update which pnodes have changed
                if (!changedNodes[oldPnode]) {
                        changedNodes[oldPnode] = [];
                }
                changedNodes[oldPnode].push(vnode);
        }

        // add pnode to pnodeList
        self.pnodes.push(node);
        self.pnodes.sort();

        log.debug('updatedRing', self.ring);
        log.info('exiting addNodes');
        self.emit('update', self.ring, changedNodes);
};

ConsistentHash.prototype.removeNode = function removeNode(pnode, cb) {
        var self = this;
        var log = self.log;

        log.info('entering remove node with %s', pnode);
        assert.string(pnode, 'pnode');
        // check that the pnode exists
        var vnodes = self.pnodeToVnodeMap_[pnode];
        if (!vnodes) {
                log.warn('pnode is not in ring', pnode);
                return cb();
        }

        if (Object.keys(vnodes).length > 0) {
                var errMsg = 'pnode still maps to vnodes, re-assign vnodes ' +
                             'first';
                log.error(errMsg);
                var err = new Error(errMsg);
                return cb(err);
        }

        // remove references to pnode.
        self.pnodes.splice(self.pnodes.indexOf(pnode));
        delete self.pnodeToVnodeMap_[pnode];
        log.info('finished removing pnode %s', pnode);
        return cb();
};

ConsistentHash.prototype.getVnodes = function getVnodes(pnode) {
        var self = this;
        var log = self.log;

        log.info('entering getVnodes with %s', pnode);
        assert.string(pnode, 'pnode');
        // check that the pnode exists
        var vnodes = self.pnodeToVnodeMap_[pnode];
        if (!vnodes) {
                log.warn('pnode is not in ring', pnode);
                return;
        }

        log.info('exiting getVnodes for pnode %s', pnode);
        log.debug('vnodes', vnodes);
        return vnodes;
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
        var valueNum = bignum(value, 16);
        log.debug('key %s hashed to hex value %s, dec value %s', key, value,
                  valueNum);

        // find the node that corresponds to this hash.
        var ringIndex = self.binarySearch(self.ring, valueNum);
        log.trace('key %s hashes to node', key, self.ring[ringIndex]);
        return {
                pnode: self.ring[ringIndex].pnode,
                vnode: self.ring[ringIndex].vnode
        };
};

/**
 * Binary searches for a value in an array. If value can't be found, returns the
 * next largest element in the array.
 *
 * @param {bignum} key The hashed key.
 * @param {Object[]} array The array to search in.
 * @param {String} array.hashspace The location of the node on the ring in hex.
 */
ConsistentHash.prototype.binarySearch = function binarySearch(array, key) {
        var log = this.log;
        var imin = 0;
        var imax = array.length - 1;
        var imid;
        // continue searching while [imin,imax] is not empty
        while (imax >= imin) {
                // calculate the midpoint for roughly equal partition
                imid = Math.floor((imin + imax) / 2);
                //log.warn('imax %s, imin %s, imid %s', imax, imin, imid);

                // determine which subarray to search
                if (array[imid]._hashspace.lt(key)) {
                        //log.warn('lt');
                        // change min index to search upper subarray
                        imin = imid + 1;
                } else if (array[imid]._hashspace.gt(key)) {
                        //log.warn('gt');
                        // change max index to search lower subarray
                        imax = imid - 1;
                } else {
                        log.trace('search for value %s index %s Returning',
                                  key, imid, array[imid]);
                        // key found at index imid
                        return imid;
                }
        }
        // key not found
        if (array[imid]._hashspace.lt(key) && (imid === array.length - 1)) {
                imid = 0;
        } else if (array[imid]._hashspace.lt(key)) {
                imid++;
        }

        log.trace('search for value %s index %s returning ',
                  key, imid, array[imid]);

        return imid;
};

// Private Functions

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


/**
 * Generate a hash given a key. Collisions are dealt with by rehashing the
 * resulting hash.
 */
function hash(self, key) {
        var log = self.log;
        log.trace('hashing key %s', key);
        // hash requires key to be a string
        var keyStr = key.toString();

        var hash = crypto.createHash(self.algorithm);
        hash.update(keyStr);
        var hashspace = hash.digest('hex');
        // check for collisions
        while (self.hashspaceToVnodeMap_[hashspace]) {
                log.warn('collision found, rehashing');
                var hash = crypto.createHash(self.algorithm);
                hash.update(hashspace);
                hashspace = hash.digest('hex');
        }

        return hashspace;
}

/**
 * add a vnode to the hash ring.
 */
function addVnode(self, key) {
        var log = self.log;
        log.trace('adding vnode %s', key);

        var hashspace;
        if (self.random) {
                var hashspace = hash(self,
                                     Math.random().toString(36).substr(2,16));
        } else {
                var hashspace = hash(self, key.toString());
        }
        self.vnodeToHashspaceMap_[key] = hashspace;
        self.hashspaceToVnodeMap_[hashspace] = key;

        log.trace('added vnode', {
                  vnode: key,
                  hashspace: bignum(hashspace, 16),
                  hash: hashspace});
}

