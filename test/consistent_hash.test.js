var bignum = require('bignum');
var crypto = require('crypto');
var fash = require('../lib');
var Logger = require('bunyan');
var tap = require('tap');
var test = tap.test;

var LOG = new Logger({
        name: 'consistent-hash-test',
        src: true,
        level: 'info'
});
var numberOfKeys = 10;
var numberOfReplicas = 2;
//var numberOfVnodes = 10000;
var numberOfVnodes = 100;

test('new ring', function(t) {
        var chash = fash.create({
                log: LOG,
                algorithm: 'sha256',
                pnodes: ['A', 'B', 'C', 'D', 'E'],
                vnodes: numberOfVnodes,
                random: true
        });

        t.equal(chash.ring.length, numberOfVnodes);
        // assert that each node appears once and only once
        var map = {};
        chash.ring.forEach(function(node) {
                var key = node._hashspace.toString(16);
                t.notOk(map[key], 'hashspace should not exist');
                map[key] = node.node;
        });

        for (var i = 0; i < numberOfKeys; i++) {
                var random = Math.random().toString(33);
                var key = random.substring(
                        Math.floor(Math.random() * random.length));
                var node = chash.getNode(key);

                var hash = crypto.createHash('sha256');
                hash.update(key);
                hash = hash.digest('hex');
                hash = bignum(hash, 16);

                var index = chash.binarySearch(chash.ring, hash);
                var prevNode;
                if (index !== 0) {
                        prevNode = chash.ring[index - 1]._hashspace;
                }

                var currNode = chash.ring[index]._hashspace;
                // assert hash is in bewtween index -1 and index
                if (index !== 0) {
                        t.ok(hash.le(currNode), 'hash ' + hash +
                             ' should be <= than ' + currNode);
                        t.ok(hash.gt(prevNode), 'hash ' + hash +
                             ' should be > than ' + prevNode + ' index is ' +
                             index + ' node ' + node);
                }

                t.ok(node);
                // assert node returned by getNode is the same as what we've
                // calculated
                t.equal(node.pnode, chash.ring[index].pnode,
                        'pnodes should match');
                t.equal(node.vnode, chash.ring[index].vnode,
                        'vnodes should match');
        }
        t.end();
});

test('add node', function(t) {
        var chash = fash.create({
                log: LOG,
                algorithm: 'sha256',
                pnodes: ['A', 'B', 'C', 'D', 'E'],
                vnodes: numberOfVnodes,
                random: true
        });
        var vnodes = [0, 1, 2, 3, 4];
        // assert update returns the pnodes and vnodes that were updated
        chash.once('update', function(ring, changedNodes) {
                var changedMap = {};
                Object.keys(changedNodes).forEach(function(pnode) {
                        for (var i = 0; i < pnode.length; i++) {
                                var vnode = changedNodes[pnode][i];
                                t.notOk(changedMap[vnode],
                                        'vnode should not yet exist');
                                t.ok(vnodes.indexOf(vnode) >= 0,
                                    'vnode should be one that has been updated');
                        }
                });
        });

        chash.remapNode('F', vnodes);
        t.equal(chash.ring.length, numberOfVnodes);
        // assert that each node appears once and only once
        // also assert that F maps to vnodes
        map = {};
        chash.ring.forEach(function(node) {
                var key = node._hashspace.toString(16);
                t.notOk(map[key], 'hashspace should not exist');
                if (node.pnode === 'F') {
                        t.ok(vnodes.indexOf(node.vnode) >= 0,
                             'F should contain vnode ' + node.vnode);
                }
                if (vnodes.indexOf(node.vnode) >= 0) {
                        t.equal('F', node.pnode, 'vnode ' + node.vnode +
                                ' should map to F');
                }
                map[key] = node;
        });

        for (var i = 0; i < numberOfKeys; i++) {
                var random = Math.random().toString(33);
                var key = random.substring(
                        Math.floor(Math.random() * random.length));
                var node = chash.getNode(key);

                var hash = crypto.createHash('sha256');
                hash.update(key);
                hash = hash.digest('hex');
                hash = bignum(hash, 16);

                var index = chash.binarySearch(chash.ring, hash);
                var prevNode;
                if (index !== 0) {
                        prevNode = chash.ring[index - 1]._hashspace;
                }

                var currNode = chash.ring[index]._hashspace;
                // assert hash is in between index -1 and index
                if (index !== 0) {
                        t.ok(hash.le(currNode), 'hash ' + hash +
                                ' should be <= than ' + currNode);
                        t.ok(hash.gt(prevNode), 'hash ' + hash +
                                ' should be > than ' + prevNode + ' index is '
                                + index + ' node ' + node);
                }

                t.ok(node);
                // assert node returned by getNode is the same as what we've
                // calculated
                t.equal(node.pnode, chash.ring[index].pnode,
                        'pnodes should match');
                t.equal(node.vnode, chash.ring[index].vnode,
                        'vnodes should match');
        }

        t.end();
});

test('remove pnode', function(t) {
        var chash = fash.create({
                log: LOG,
                algorithm: 'sha256',
                vnodes: numberOfVnodes,
                pnodes: ['A', 'B', 'C', 'D', 'E'],
                random: true
        });
        var vnodes = chash.getVnodes('B');
        // clone vnodes since it gets destroyed on removeNode
        var bnodes = {};
        for (var i in vnodes) {
                bnodes[i] = true;
        }
        chash.remapNode('A', Object.keys(vnodes));
        chash.removeNode('B', function(err) {
                t.notOk(err);
                t.equal(chash.ring.length, numberOfVnodes);
                t.notOk(chash.getVnodes('B'));
                // assert that each node appears once and only once
                var map = {};
                chash.ring.forEach(function(node) {
                        t.notOk(node.pnode  === 'B');
                        var key = node._hashspace.toString(16);
                        t.notOk(map[key], 'hashspace should not exist');
                        map[key] = node.node;
                        if (bnodes[node.vnode]) {
                                t.equal('A', node.pnode, 'vnode ' + node.vnode +
                                ' should map to A');
                        }
                });

                for (var i = 0; i < numberOfKeys; i++) {
                        var random = Math.random().toString(33);
                        var key = random.substring(
                                Math.floor(Math.random() * random.length));
                                var node = chash.getNode(key);

                                var hash = crypto.createHash('sha256');
                                hash.update(key);
                                hash = hash.digest('hex');
                                hash = bignum(hash, 16);

                                var index = chash.binarySearch(chash.ring,
                                                               hash);
                                var prevNode;
                                if (index !== 0) {
                                        prevNode = chash.ring[index - 1].
                                                _hashspace;
                                }

                                var currNode = chash.ring[index]._hashspace;
                                // assert hash is in bewtween index -1 and index
                                if (index !== 0) {
                                        t.ok(hash.le(currNode), 'hash ' + hash +
                                        ' should be <= than ' + currNode);
                                t.ok(hash.gt(prevNode), 'hash ' + hash +
                                        ' should be > than ' + prevNode +
                                        ' index is ' + index + ' node ' + node);
                                }

                                t.ok(node);
                                // assert node returned by getNode is the same
                                // as what we've calculated
                                t.equal(node.pnode, chash.ring[index].pnode,
                                        'pnodes should match');
                                t.equal(node.vnode, chash.ring[index].vnode,
                                        'vnodes should match');
                }
                t.end();
        });
});

test('instantiate from persisted toplogy', function(t) {
        var chash = fash.create({
                log: LOG,
                algorithm: 'sha256',
                vnodes: numberOfVnodes,
                pnodes: ['A', 'B', 'C', 'D', 'E'],
                random: true
        });
        //var ring = chash.ring;
        var ring = chash.serialize();
        var chash2 = fash.deserialize({
                log: LOG,
                algorithm: 'sha256',
                topology: chash.ring
        });

        t.ok(chash2);
        t.equal(chash2.ring.length, chash.ring.length, 'ring sizes should be identical');
        chash2.ring.forEach(function(node, index) {
                var node2 = chash2.ring[index];
                t.ok(node._hashspace.eq(node2._hashspace));
                t.equal(node.hashspace, node2.hashspace);
                t.equal(node.node, node2.node);
                t.equal(node.vnode, node2.vnode);
        });
        for (var i = 0; i < numberOfKeys; i++) {
                var random = Math.random().toString(33);
                var key = random.substring(Math.floor(Math.random() * random.length));
                var node1 = chash.getNode(key);
                var node2 = chash2.getNode(key);
                t.equal(node1.pnode, node2.pnode);
                t.equal(node1.vnode, node2.vnode);
        }

        // add a pnode to both chashes
        var vnodes = [0, 1, 2, 3, 4];
        chash.remapNode('F', vnodes);
        chash2.remapNode('F', vnodes);

        t.equal(chash2.ring.length, chash.ring.length, 'ring sizes should be identical');
        chash2.ring.forEach(function(node, index) {
                var node2 = chash2.ring[index];
                t.ok(node._hashspace.eq(node2._hashspace));
                t.equal(node.hashspace, node2.hashspace);
                t.equal(node.node, node2.node);
                t.equal(node.vnode, node2.vnode);
        });
        for (var i = 0; i < numberOfKeys; i++) {
                var random = Math.random().toString(33);
                var key = random.substring(Math.floor(Math.random() * random.length));
                var node1 = chash.getNode(key);
                var node2 = chash2.getNode(key);
                t.equal(node1.pnode, node2.pnode);
                t.equal(node1.vnode, node2.vnode);
        }

        // let's remove B
        vnodes = chash.getVnodes('B');
        // and assign B's nodes to A
        chash.remapNode('A', Object.keys(vnodes));
        var vnodes2 = chash2.getVnodes('B');
        chash2.remapNode('A', Object.keys(vnodes2));
        chash.removeNode('B', function(err) {
                t.notOk(err);
                chash2.removeNode('B', function(err) {
                        t.notOk(err);
                        t.equal(chash2.ring.length, chash.ring.length, 'ring sizes should be identical');
                        chash.ring.forEach(function(node, index) {
                                var node2 = chash2.ring[index];
                                t.ok(node._hashspace.eq(node2._hashspace));
                                t.equal(node.hashspace, node2.hashspace);
                                t.equal(node.pnode, node2.pnode);
                                t.equal(node.vnode, node2.vnode);
                        });
                        for (var i = 0; i < numberOfKeys; i++) {
                                var random = Math.random().toString(33);
                                var key = random.substring(Math.floor(Math.random() * random.length));
                                var node1 = chash.getNode(key);
                                var node2 = chash2.getNode(key);
                                t.equal(node1.pnode, node2.pnode);
                                t.equal(node1.vnode, node2.vnode);
                        }

                        t.end();
                });
        });
});


test('hashing the same key', function(t) {
        var chash = fash.create({
                log: LOG,
                algorithm: 'sha256',
                vnodes: numberOfVnodes,
                pnodes: ['A', 'B', 'C', 'D', 'E'],
                random: true
        });
        for (var i = 0; i < 10; i++) {
                var random = Math.random().toString(33);
                var key = random.substring(Math.floor(Math.random() * random.length));
                var node1 = chash.getNode(key);
                var node2 = chash.getNode(key);
                t.equal(node1.pnode, node2.pnode);
                t.equal(node1.vnode, node2.vnode);
        }

        t.end();
});

test('collision', function(t) {
        var caught;
        try {
                var chash = fash.create({
                        log: LOG,
                        algorithm: 'sha256',
                        pnodes: ['a', 'a'],
                        vnodes: numberOfVnodes,
                        random: true
                });
        } catch (e) {
                caught = true;
        }
        t.ok(caught, 'collision of pnodes should throw');
        t.end();
});

tap.tearDown(function() {
        process.exit(tap.output.results.fail);
});

