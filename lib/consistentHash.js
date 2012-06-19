var assert = require('./assert');
var bignum = require('bignum');
var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var sprintf = util.format;

var assertArray = assert.assertArray;
var assertFunction = assert.assertFunction;
var assertNumber = assert.assertNumber;
var assertObject = assert.assertObject;
var assertString = assert.assertString;

function ConsistentHash(options) {
  assertObject('options', options);
  assertObject('options.log', options.log);
  assertString('options.algorithm', options.algorithm);
  /**
   * Nodes in the ring sorted by ORDER of insertion OR the ring topology.
   */
  assertArray('options.nodes', ['string', 'object'], options.nodes);
  assertNumber('options.numberOfReplicas', options.numberOfReplicas);

  EventEmitter.call(this);

  this.nodes = options.nodes;
  this.numberOfReplicas = options.numberOfReplicas;
  this.algorithm = options.algorithm;
  this.log = options.log;
  var self = this;
  var log = self.log;

  log.info('new ConsistentHash with options', options);

  /**
   * Map of {node1 -> [hashSpace, ...], ...}
   * keeps track of the hashspace that each physical node owns. Used for quick
   * lookups on add/remove node
   */
  this.nodeMap = {};

  /**
   * The map of hashspace -> node, used for quick lookups for collision
   * detection and node removal.
   */
  this.ringMap= {};

  /**
   * The sorted array of {hashSpace, node, replica} that represents the hash ring.
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
module.exports = ConsistentHash;
util.inherits(ConsistentHash, EventEmitter);

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

ConsistentHash.prototype.getNode = function getNode(key) {
  var self = this;
  var log = self.log;

  var hash = crypto.createHash(self.algorithm);
  hash.update(key);
  var value = hash.digest('hex');
  var valueNum = bignum(value, 16);
  log.debug('key %s hashed to value %s hex, %s dec', key, value, valueNum);

  // find the node that corresponds to this hash.
  var ringIndex = self.binarySearch(self.ring, valueNum);
  log.debug('key %s hashes to node', key, self.ring[ringIndex]);
  return self.ring[ringIndex].node;
};

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
    log.debug('removing ring idx %s, entry', index, ring[index]);
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
 * @param {bignum} value
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
      log.debug('search for value %s index %s Returning', key, imid, array[imid]);
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
  log.debug('search for value %s index %s returning ', key, imid, array[imid]);

  return imid;
};

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

function hash(self, node) {
  var log = self.log;
  log.debug('hashing key %s', node);
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
    log.info('pushing into ring', {hashSpace: bignum(hashSpace, 16), node: node});
    var idx = self.ring.push({
      hashSpace: hashSpace,
      _hashSpace: bignum(hashSpace, 16),
      node: node,
      replica: nodeName}) - 1;
    self.nodeMap[node].push(bignum(hashSpace, 16));
  }
}

