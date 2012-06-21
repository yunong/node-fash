var bignum = require('bignum');
var crypto = require('crypto');
var fash = require('../lib');
var Logger = require('bunyan');
var tap = require('tap');
var test = tap.test;

var LOG = new Logger({
  name: 'consistent-hash-test',
  src: true,
  level: 'trace'
});

var numberOfKeys = 1;
var numberOfReplicas = 2;
var chash;
var chash2;

test('mapper', function(t) {
  var nodes = ['A', 'B', 'C', 'D', 'E'];
  chash = fash.createHash({
    log: LOG,
    algorithm: 'sha256',
    nodes: nodes,
    numberOfReplicas: numberOfReplicas
  });

  t.equal(chash.ring.length, 5 * numberOfReplicas);
  var map = {};
  chash.ring.forEach(function(node) {
    var key = node._hashSpace.toString(16);
    t.notOk(map[key], 'hashspace should not exist');
    map[key] = node.node;
  });
  for (var i = 0; i < numberOfKeys; i++) {
    var random = Math.random().toString(33);
    var key = random.substring(Math.floor(Math.random() * random.length));
    var node = chash.getNode(key);

    var hash = crypto.createHash('sha256');
    hash.update(key);
    hash = hash.digest('hex');
    hash = bignum(hash, 16);

    var index = chash.binarySearch(chash.ring, hash);
    var prevNode;
    if (index !== 0) {
      prevNode = chash.ring[index - 1]._hashSpace;
    }

    var currNode = chash.ring[index]._hashSpace;
    // assert hash is in bewtween index -1 and index
    if (index !== 0) {
      t.ok(hash.le(currNode), 'hash ' + hash + ' should be <= than ' + currNode);
      t.ok(hash.gt(prevNode), 'hash ' + hash + ' should be > than ' + prevNode + ' index is ' + index + ' node ' + node);
    }

    t.ok(node);
  }

  chash.addNode('F');
  t.equal(chash.ring.length, 6 * numberOfReplicas);
  map = {};
  chash.ring.forEach(function(node) {
    var key = node._hashSpace.toString(16);
    t.notOk(map[key], 'hashspace should not exist');
    map[key] = node.node;
  });
  for (var i = 0; i < numberOfKeys; i++) {
    var random = Math.random().toString(33);
    var key = random.substring(Math.floor(Math.random() * random.length));
    var node = chash.getNode(key);

    var hash = crypto.createHash('sha256');
    hash.update(key);
    hash = hash.digest('hex');
    hash = bignum(hash, 16);

    var index = chash.binarySearch(chash.ring, hash);
    var prevNode;
    if (index !== 0) {
      prevNode = chash.ring[index - 1]._hashSpace;
    }

    var currNode = chash.ring[index]._hashSpace;
    // assert hash is in bewtween index -1 and index
    if (index !== 0) {
      t.ok(hash.le(currNode), 'hash ' + hash + ' should be <= than ' + currNode);
      t.ok(hash.gt(prevNode), 'hash ' + hash + ' should be > than ' + prevNode + ' index is ' + index + ' node ' + node);
    }

    t.ok(node);
  }

  chash.removeNode('A');
  t.equal(chash.ring.length, 5 * numberOfReplicas);
  map = {};
  chash.ring.forEach(function(node) {
    var key = node._hashSpace.toString(16);
    t.notOk(map[key], 'hashspace should not exist');
    map[key] = node.node;
  });

  for (var i = 0; i < numberOfKeys; i++) {
    var random = Math.random().toString(33);
    var key = random.substring(Math.floor(Math.random() * random.length));
    var node = chash.getNode(key);

    var hash = crypto.createHash('sha256');
    hash.update(key);
    hash = hash.digest('hex');
    hash = bignum(hash, 16);

    var index = chash.binarySearch(chash.ring, hash);
    var prevNode;
    if (index !== 0) {
      prevNode = chash.ring[index - 1]._hashSpace;
    }

    var currNode = chash.ring[index]._hashSpace;
    // assert hash is in bewtween index -1 and index
    if (index !== 0) {
      t.ok(hash.le(currNode), 'hash ' + hash + ' should be <= than ' + currNode);
      t.ok(hash.gt(prevNode), 'hash ' + hash + ' should be > than ' + prevNode + ' index is ' + index + ' node ' + node);
    }

    t.ok(node);
  }

  chash.addNode('DIFFERENT_REPLICAS', 100);
  t.equal(chash.ring.length, 5 * numberOfReplicas + 100);
  map = {};
  chash.ring.forEach(function(node) {
    var key = node._hashSpace.toString(16);
    t.notOk(map[key], 'hashspace should not exist');
    map[key] = node.node;
  });

  for (var i = 0; i < numberOfKeys; i++) {
    var random = Math.random().toString(33);
    var key = random.substring(Math.floor(Math.random() * random.length));
    var node = chash.getNode(key);

    var hash = crypto.createHash('sha256');
    hash.update(key);
    hash = hash.digest('hex');
    hash = bignum(hash, 16);

    var index = chash.binarySearch(chash.ring, hash);
    var prevNode;
    if (index !== 0) {
      prevNode = chash.ring[index - 1]._hashSpace;
    }

    var currNode = chash.ring[index]._hashSpace;
    // assert hash is in bewtween index -1 and index
    if (index !== 0) {
      t.ok(hash.le(currNode), 'hash ' + hash + ' should be <= than ' + currNode);
      t.ok(hash.gt(prevNode), 'hash ' + hash + ' should be > than ' + prevNode + ' index is ' + index + ' node ' + node);
    }

    t.ok(node);
  }

  chash.removeNode('DIFFERENT_REPLICAS');
  t.equal(chash.ring.length, 5 * numberOfReplicas);
  map = {};
  chash.ring.forEach(function(node) {
    var key = node._hashSpace.toString(16);
    t.notOk(map[key], 'hashspace should not exist');
    map[key] = node.node;
  });

  for (var i = 0; i < numberOfKeys; i++) {
    var random = Math.random().toString(33);
    var key = random.substring(Math.floor(Math.random() * random.length));
    var node = chash.getNode(key);

    var hash = crypto.createHash('sha256');
    hash.update(key);
    hash = hash.digest('hex');
    hash = bignum(hash, 16);

    var index = chash.binarySearch(chash.ring, hash);
    var prevNode;
    if (index !== 0) {
      prevNode = chash.ring[index - 1]._hashSpace;
    }

    var currNode = chash.ring[index]._hashSpace;
    // assert hash is in bewtween index -1 and index
    if (index !== 0) {
      t.ok(hash.le(currNode), 'hash ' + hash + ' should be <= than ' + currNode);
      t.ok(hash.gt(prevNode), 'hash ' + hash + ' should be > than ' + prevNode + ' index is ' + index + ' node ' + node);
    }

    t.ok(node);
  }
  t.end();
});

test('instantiate from persisted toplogy', function(t) {
  var ring = chash.ring;
  chash2 = fash.createHash({
    log: LOG,
    algorithm: 'sha256',
    nodes: ring,
    numberOfReplicas: 2
  });
  LOG.info('chash2', chash2.nodeMap);
  LOG.info('chash', chash.nodeMap);
  LOG.info('chash ring', chash.ring);
  LOG.info('chash2 ring', chash2.ring);

  t.ok(chash2);
  t.equal(chash2.ring.length, chash.ring.length, 'ring sizes should be identical');
  chash.ring.forEach(function(node, index) {
    var node2 = chash2.ring[index];
    t.ok(node._hashSpace.eq(node2._hashSpace));
    t.equal(node.hashSpace, node2.hashSpace);
    t.equal(node.node, node2.node);
    t.equal(node.replica, node2.replica);
  });
  for (var i = 0; i < numberOfKeys; i++) {
    var random = Math.random().toString(33);
    var key = random.substring(Math.floor(Math.random() * random.length));
    var node1 = chash.getNode(key);
    var node2 = chash2.getNode(key);
    t.equal(node1, node2);
  }

  chash.addNode('FOOBAR');
  chash2.addNode('FOOBAR');
  t.equal(chash2.ring.length, chash.ring.length, 'ring sizes should be identical');
  chash.ring.forEach(function(node, index) {
    var node2 = chash2.ring[index];
    t.ok(node._hashSpace.eq(node2._hashSpace));
    t.equal(node.hashSpace, node2.hashSpace);
    t.equal(node.node, node2.node);
    t.equal(node.replica, node2.replica);
  });
  for (var i = 0; i < numberOfKeys; i++) {
    var random = Math.random().toString(33);
    var key = random.substring(Math.floor(Math.random() * random.length));
    var node1 = chash.getNode(key);
    var node2 = chash2.getNode(key);
    t.equal(node1, node2);
  }

  chash.removeNode('B');
  chash2.removeNode('B');
  t.equal(chash2.ring.length, chash.ring.length, 'ring sizes should be identical');
  LOG.info('chash', chash.ring);
  LOG.info('chash2', chash2.ring);
  chash.ring.forEach(function(node, index) {
    var node2 = chash2.ring[index];
    t.ok(node._hashSpace.eq(node2._hashSpace));
    t.equal(node.hashSpace, node2.hashSpace);
    t.equal(node.node, node2.node);
    t.equal(node.replica, node2.replica);
  });
  for (var i = 0; i < numberOfKeys; i++) {
    var random = Math.random().toString(33);
    var key = random.substring(Math.floor(Math.random() * random.length));
    var node1 = chash.getNode(key);
    var node2 = chash2.getNode(key);
    t.equal(node1, node2);
  }
  t.end();
});

test('hashing the same key', function(t) {
  for (var i = 0; i < 10; i++) {
    var random = Math.random().toString(33);
    var key = random.substring(Math.floor(Math.random() * random.length));
    var node1 = chash.getNode(key);
    var node2 = chash.getNode(key);
    t.equal(node1, node2);
  }

  t.end();
});

test('collision', function(t) {
  var nodes = ['a', 'a'];
  chash = fash.createHash({
    log: LOG,
    algorithm: 'sha256',
    nodes: nodes,
    numberOfReplicas: 2
  });

  t.equal(chash.ring.length, 4, 'number of nodes in the ring should be 4');
  var map = {};
  chash.ring.forEach(function(node) {
    var key = node._hashSpace.toString(16);
    t.notOk(map[key], 'hashspace should not exist');
    map[key] = node.node;
  });
  t.end();
});

tap.tearDown(function() {
  process.exit(tap.output.results.fail);
});

