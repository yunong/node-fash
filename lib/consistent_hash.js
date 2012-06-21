/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */
var assert = require('./assert');
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
 * @param {String} hashSpace The position on the ring the node occupies in hex
 *                 format.
 */
function ConsistentHash(options) {
  assertObject('options', options);
  assertString('options.algorithm', options.algorithm);
  assertArray('options.nodes', ['string', 'object'], options.nodes);
  assertNumber('options.numberOfReplicas', options.numberOfReplicas);

  EventEmitter.call(this);

  this.nodes = options.nodes;
  this.numberOfReplicas = options.numberOfReplicas;
  this.algorithm = options.algorithm;
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
   * Map of {node1 -> [hashSpace, ...], ...}
   * keeps track of the hashspace that each physical node owns. Used for quick
   * lookups on add/remove node
   */
  this.nodeMap = {};

  /**
   * The map of {hashspace -> node, ...}, used for quick lookups for collision
   * detection and node removal.
   */
  this.ringMap= {};

  /**
   * The sorted array of [{hashSpace, node, replica}, ... ] that represents the
   * hash ring. This can be used to persist the topology of the hash ring and
   * instantiate a new client with the same toplogy.
   */
  this.ring = [];

  // Provision a new ring with a set of nodes.
  if (typeof(options.nodes[0]) === 'string') {
    self.nodes.forEach(function(node, index) {
      self.nodeMap[node] = [];
    });

    /**
    * Construct the hashspace as follows;
    * 1) Hash each node in order. The resulting hash is the node's place on the
    * ring.
    * 2) If there is a collision, then hash(hash) until no collisions are found.
    */
    options.nodes.forEach(function(node, index) {
      hashAndAdd(self, node, options.numberOfReplicas);
    });

    // sort the ring
    self.ring.sort(sort);
  } else { // Provision a ring with a given topology
    // clone the topology
    options.nodes.forEach(function(node, index) {
      // parse the hashspace, since it's serialized as a string
      var hashSpace = bignum(node.hashSpace, 16);
      // add to ring
      self.ring.push({
        hashSpace: node.hashSpace,
        _hashSpace: hashSpace,
        node: node.node,
        replica: node.replica
      });
      var nodeName = node.node;

      // populate the nodeMap
      if (!self.nodeMap[nodeName]) {
        self.nodeMap[nodeName] = [];
      }
      self.nodeMap[nodeName].push(hashSpace);

      // populate the ringMap
      self.ringMap[node.hashSpace] = nodeName;
    });
  }
}

/**
 * @exports ConsistentHash as Consistenthash
 */
module.exports = ConsistentHash;
util.inherits(ConsistentHash, EventEmitter);

/**
 * Adds a node to the hash ring. Emits an 'update' event with the ring toplogy
 * when done.
 *
 * @param {String} node The name of the node.
 * @param [Number=options.numberOfReplicas] numberOfReplicas The optional number
 *                                          of replicas to provision for this
 *                                          node, defaults to the number
 *                                          specified in the initial cfg.
 *
 * @event update
 * @param {Object[]} ring The updated ring toplogy.
 * @param {String} ring.hashSpace The location of the node on the ring in hex.
 * @param {String} ring.node The name of the physical node.
 * @param {String} ring.replica The replica name of the node.
 */
ConsistentHash.prototype.addNode = function addNode(node, numberOfReplicas) {
  var self = this;
  var log = self.log;
  if (!numberOfReplicas) {
    numberOfReplicas = self.numberOfReplicas;
  }
  // check for dupes
  if (self.nodeMap[node]) {
    log.warn('node already exists');
    return;
  }

  // add to nodeMap
  self.nodeMap[node] = [];

  // hash and add to ringMap
  hashAndAdd(self, node, numberOfReplicas);

  self.ring.sort(sort);
  self.emit('update', self.ring);
};

/**
 * Gets the node that a key belongs to on the ring.
 *
 * @param {String} key The key.
 *
 * @returns {String} node The node that the key maps to.
 */
ConsistentHash.prototype.getNode = function getNode(key) {
  var self = this;
  var log = self.log;

  var hash = crypto.createHash(self.algorithm);
  hash.update(key);
  var value = hash.digest('hex');
  var valueNum = bignum(value, 16);
  log.trace('key %s hashed to value %s hex, %s dec', key, value, valueNum);

  // find the node that corresponds to this hash.
  var ringIndex = self.binarySearch(self.ring, valueNum);
  log.trace('key %s hashes to node', key, self.ring[ringIndex]);
  return self.ring[ringIndex].node;
};

/**
 * Removes a node and its replicas from the ring. Emits an 'update' event with
 * the ring toplogy when done.
 *
 * @param {String} node The name of the node.
 *
 * @event update
 * @param {Object[]} ring The updated ring toplogy.
 * @param {String} ring.hashSpace The location of the node on the ring in hex.
 * @param {String} ring.node The name of the physical node.
 * @param {String} ring.replica The replica name of the node.
 */
ConsistentHash.prototype.removeNode = function removeNode(node) {
  var self = this;
  var log = self.log;
  var nodeMap = self.nodeMap;
  var ring = self.ring;
  var ringMap = self.ringMap;

  if (!nodeMap[node]) {
    log.warn('node %s doesn\'t exist, silently returning', name);
    return;
  }

  // remove all replicas from ringMap and ring
  nodeMap[node].forEach(function(hashSpace) {
    var index = self.binarySearch(ring, hashSpace);
    log.trace('removing ring idx %s, entry', index, ring[index]);
    ring.splice(index, 1);
    delete ringMap[hashSpace];
  });

  // remove from nodeMap
  delete nodeMap[node];
  self.emit('update', self.ring);
};

/**
 * Binary searches for value in an array. If value can't be found, returns the
 * next largest element in the array.
 *
 * @param {bignum} key The hashed key.
 * @param {Object[]} array The array to search in.
 * @param {String} array.hashSpace The location of the node on the ring in hex.
 */
ConsistentHash.prototype.binarySearch = function binarySearch(array, key) {
  var log = this.log;
  var imin = 0;
  var imax = array.length - 1;
  var imid;
  // continue searching while [imin,imax] is not empty
  while (imax >= imin) {
    /* calculate the midpoint for roughly equal partition */
    imid = Math.floor((imin + imax) / 2);

    // determine which subarray to search
    if (array[imid]._hashSpace.lt(key)) {
      // change min index to search upper subarray
      imin = imid + 1;
    } else if (array[imid]._hashSpace.gt(key)) {
      // change max index to search lower subarray
      imax = imid - 1;
    } else {
      log.trace('search for value %s index %s Returning', key, imid, array[imid]);
      // key found at index imid
      return imid;
    }
  }
  // key not found
  if (array[imid]._hashSpace.lt(key) && (imid === array.length - 1)) {
    imid = 0;
  } else if (array[imid]._hashSpace.lt(key)) {
    imid++;
  }

  log.trace('search for value %s index %s returning ', key, imid, array[imid]);

  return imid;
};

// Private Functions

/**
 * sorts the hashring from the smallest integer to the largest.
 */
function sort(a, b) {
  a = a._hashSpace;
  b = b._hashSpace;
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
function hash(self, node) {
  var log = self.log;
  log.trace('hashing key %s', node);

  var hash = crypto.createHash(self.algorithm);
  hash.update(node);
  var hashSpace = hash.digest('hex');
  // check for collisions
  while (self.ringMap[hashSpace]) {
    log.warn('collision found, rehashing');
    var hash = crypto.createHash(self.algorithm);
    hash.update(hashSpace);
    hashSpace = hash.digest('hex');
  }

  return hashSpace;
}

/**
 * add a node and its replicas to the hash ring.
 */
function hashAndAdd(self, node, numberOfReplicas) {
  var log = self.log;
  for (var i = 0; i < numberOfReplicas; i++) {
    var nodeName = sprintf('%s-%s', node, i);
    var hashSpace = hash(self, nodeName);
    self.ringMap[hashSpace] = node;
    log.trace('pushing into ring', {hashSpace: bignum(hashSpace, 16), node: node});
    var idx = self.ring.push({
      hashSpace: hashSpace,
      _hashSpace: bignum(hashSpace, 16),
      node: node,
      replica: nodeName}) - 1;
    self.nodeMap[node].push(bignum(hashSpace, 16));
  }
}

