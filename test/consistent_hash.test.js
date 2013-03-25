var bignum = require('bignum');
var crypto = require('crypto');
var fash = require('../lib');
var Logger = require('bunyan');
var util = require('util');
var tap = require('tap');
var test = tap.test;

var LOG = new Logger({
    name: 'consistent-hash-test',
    src: true,
    //level: 'info',
    //level: 'debug',
    level: 'trace',
    //level: process.LOG_LEVEL || 'info'
});
var numberOfKeys = 100;
var numberOfReplicas = 2;
//var numberOfVnodes = 10000;
var numberOfVnodes = 100;

test('new ring', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: 'sha256',
        algorithmMax: fash.SHA_256_MAX,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: numberOfVnodes,
    });

    _verifyRing(chash, t, function() {
        t.end();
    });
});

test('remapOnePnodeToAnother', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: 'sha256',
        algorithmMax: fash.SHA_256_MAX,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: numberOfVnodes,
    });

    var beforeRing = chash.serialize();
    // get vnodes from A
    var aVnodes = chash.getVnodes('A');

    // remap all to B
    chash.remapVnode('B', aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var afterRing = chash.serialize();
        afterRing.forEach(function(elem) {
            t.ok((elem.pnode.toString() !== 'A'), 'pnode A should not exist in ring');
        });

        beforeRing.forEach(function(elem, index, array) {
            if (elem.pnode.toString() === 'A') {
                t.ok(afterRing[index].pnode.toString() === 'B',
                     'vnode ' + index + 'should belong to B');
            }
        });

        _verifyRing(chash, t, function() {
            t.end();
        });
    });
});

test('remapSomeVnodeToAnother', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: 'sha256',
        algorithmMax: fash.SHA_256_MAX,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: numberOfVnodes,
    });

    var beforeRing = chash.serialize();
    // get vnodes from A
    var aVnodes = chash.getVnodes('A');
    aVnodes = aVnodes.slice(aVnodes.length / 2);

    // remap some of A's vnodes to B
    chash.remapVnode('B', aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var afterRing = chash.serialize();
        afterRing.forEach(function(elem) {
            if (elem.pnode.toString() === 'A') {
                aVnodes.forEach(function(remappedVnode) {
                    t.ok(elem.vnode.toString() !== remappedVnode.toString(),
                         'remapped vnode ' + remappedVnode + ' should not belong to A');
                });
            }
        });

        aVnodes.forEach(function(remappedVnode) {
            var pnode = afterRing[parseInt(remappedVnode, 10)].pnode.toString();
            t.ok(pnode === 'B', 'vnode ' + remappedVnode + 'should belong to B');
        });

        _verifyRing(chash, t, function() {
            t.end();
        });
    });
});

test('removePnode', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: 'sha256',
        algorithmMax: fash.SHA_256_MAX,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: numberOfVnodes,
    });

    // remap all of A to B
    chash.remapVnode('B', chash.getVnodes('A'), function(err, ring, pnodes) {
        t.ifErr(err);

        chash.removePnode('A', function(err, ring) {
            t.ifErr(err);
            t.ok(chash.pnodes.indexOf('A') === -1, 'A should not exist in pnode array');
            t.notOk(chash.pnodeToVnodeMap_['A'], 'A should not exist in pnodeToVnodeMap');
            _verifyRing(chash, t, function() {
                t.end();
            });
        });
    });
});

test('add new pnode', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: 'sha256',
        algorithmMax: fash.SHA_256_MAX,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: numberOfVnodes,
    });

    var beforeRing = chash.serialize();
    // get vnodes from A
    var aVnodes = chash.getVnodes('A');

    // remap all to F
    chash.remapVnode('F', aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var afterRing = chash.serialize();
        afterRing.forEach(function(elem) {
            t.ok((elem.pnode.toString() !== 'A'), 'pnode A should not exist in ring');
        });

        beforeRing.forEach(function(elem, index, array) {
            if (elem.pnode.toString() === 'A') {
                t.ok(afterRing[index].pnode.toString() === 'F',
                     'vnode ' + index + 'should belong to F');
            }
        });

        _verifyRing(chash, t, function() {
            t.end();
        });
    });
});

test('add new pnode -- remap only subset of old pnode', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: 'sha256',
        algorithmMax: fash.SHA_256_MAX,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: numberOfVnodes,
    });

    var beforeRing = chash.serialize();
    // get vnodes from A
    var aVnodes = chash.getVnodes('A');
    aVnodes = aVnodes.slice(aVnodes.length / 2);

    // remap some of A's vnodes to F
    chash.remapVnode('F', aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var afterRing = chash.serialize();
        afterRing.forEach(function(elem) {
            if (elem.pnode.toString() === 'A') {
                aVnodes.forEach(function(remappedVnode) {
                    t.ok(elem.vnode.toString() !== remappedVnode.toString(),
                         'remapped vnode ' + remappedVnode + ' should not belong to A');
                });
            }
        });

        aVnodes.forEach(function(remappedVnode) {
            var pnode = afterRing[parseInt(remappedVnode, 10)].pnode.toString();
            t.ok(pnode === 'F', 'vnode ' + remappedVnode + 'should belong to F');
        });
        _verifyRing(chash, t, function() {
            t.end();
        });
    });
});

//test('add node', function(t) {
    //var chash = fash.create({
        //log: LOG,
        //algorithm: 'sha256',
        //algorithmMax: fash.SHA_256_MAX,
        //pnodes: ['A', 'B', 'C', 'D', 'E'],
        //vnodes: numberOfVnodes,
        //random: true
    //});
    //var vnodes = [0, 1, 2, 3, 4];
    //// assert update returns the pnodes and vnodes that were updated
    //chash.once('update', function(ring, changedNodes) {
        //var changedMap = {};
        //Object.keys(changedNodes).forEach(function(pnode) {
            //for (var i = 0; i < pnode.length; i++) {
                //var vnode = changedNodes[pnode][i];
                //t.notOk(changedMap[vnode], 'vnode should not yet exist');
                //t.ok(vnodes.indexOf(vnode) >= 0, 'vnode should be one that has been updated');
            //}
        //});
    //});

    //chash.remapNode('F', vnodes);
    //t.equal(chash.ring_.length, numberOfVnodes);
    //// assert that each node appears once and only once also assert that F maps
    //// to vnodes
    //map = {};
    //chash.ring_.forEach(function(node) {
        //var key = node._hashspace.toString(16);
        //t.notOk(map[key], 'hashspace should not exist');
        //if (node.pnode === 'F') {
            //t.ok(vnodes.indexOf(node.vnode) >= 0,
            //'F should contain vnode ' + node.vnode);
        //}
        //if (vnodes.indexOf(node.vnode) >= 0) {
            //t.equal('F', node.pnode, 'vnode ' + node.vnode +
            //' should map to F');
        //}
        //map[key] = node;
    //});

    //for (var i = 0; i < numberOfKeys; i++) {
        //var random = Math.random().toString(33);
        //var key = random.substring(
            //Math.floor(Math.random() * random.length));
            //var node = chash.getNode(key);

            //var hash = crypto.createHash('sha256');
            //hash.update(key);
            //hash = hash.digest('hex');
            //hash = bignum(hash, 16);

            //var index = chash.binarySearch(chash.ring_, hash);
            //var prevNode;
            //if (index !== 0) {
                //prevNode = chash.ring_[index - 1]._hashspace;
            //}

            //var currNode = chash.ring_[index]._hashspace;
            //// assert hash is in between index -1 and index
            //if (index !== 0) {
                //t.ok(hash.le(currNode), 'hash ' + hash +
                //' should be <= than ' + currNode);
            //t.ok(hash.gt(prevNode), 'hash ' + hash +
                //' should be > than ' + prevNode + ' index is '
            //+ index + ' node ' + node);
            //}

            //t.ok(node);
            //// assert node returned by getNode is the same as what we've
            //// calculated
            //t.equal(node.pnode, chash.ring_[index].pnode,
            //'pnodes should match');
            //t.equal(node.vnode, chash.ring_[index].vnode,
            //'vnodes should match');
    //}

    //t.end();
//});

//test('remove pnode', function(t) {
    //var chash = fash.create({
        //log: LOG,
        //algorithm: 'sha256',
        //algorithmMax: fash.SHA_256_MAX,
        //vnodes: numberOfVnodes,
        //pnodes: ['A', 'B', 'C', 'D', 'E'],
        //random: true
    //});
    //var vnodes = chash.getVnodes('B');
    //// clone vnodes since it gets destroyed on removeNode
    //var bnodes = {};
    //for (var i in vnodes) {
        //bnodes[i] = true;
    //}
    //chash.remapNode('A', Object.keys(vnodes));
    //chash.removeNode('B', function(err) {
        //t.notOk(err);
        //t.equal(chash.ring_.length, numberOfVnodes);
        //t.notOk(chash.getVnodes('B'));
        //// assert that each node appears once and only once
        //var map = {};
        //chash.ring_.forEach(function(node) {
            //t.notOk(node.pnode  === 'B');
            //var key = node._hashspace.toString(16);
            //t.notOk(map[key], 'hashspace should not exist');
            //map[key] = node.node;
            //if (bnodes[node.vnode]) {
                //t.equal('A', node.pnode, 'vnode ' + node.vnode +
                //' should map to A');
            //}
        //});

        //for (var i = 0; i < numberOfKeys; i++) {
            //var random = Math.random().toString(33);
            //var key = random.substring(
                //Math.floor(Math.random() * random.length));
                //var node = chash.getNode(key);

                //var hash = crypto.createHash('sha256');
                //hash.update(key);
                //hash = hash.digest('hex');
                //hash = bignum(hash, 16);

                //var index = chash.binarySearch(chash.ring_,
                //hash);
                //var prevNode;
                //if (index !== 0) {
                    //prevNode = chash.ring_[index - 1].
                    //_hashspace;
                //}

                //var currNode = chash.ring_[index]._hashspace;
                //// assert hash is in bewtween index -1 and index
                //if (index !== 0) {
                    //t.ok(hash.le(currNode), 'hash ' + hash +
                    //' should be <= than ' + currNode);
                //t.ok(hash.gt(prevNode), 'hash ' + hash +
                    //' should be > than ' + prevNode +
                //' index is ' + index + ' node ' + node);
                //}

                //t.ok(node);
                //// assert node returned by getNode is the same
                //// as what we've calculated
                //t.equal(node.pnode, chash.ring_[index].pnode,
                //'pnodes should match');
                //t.equal(node.vnode, chash.ring_[index].vnode,
                //'vnodes should match');
        //}
        //t.end();
    //});
//});

//test('instantiate from persisted toplogy', function(t) {
    //var chash = fash.create({
        //log: LOG,
        //algorithm: 'sha256',
        //algorithmMax: fash.SHA_256_MAX,
        //vnodes: numberOfVnodes,
        //pnodes: ['A', 'B', 'C', 'D', 'E'],
        //random: true
    //});
    ////var ring = chash.ring_;
    //var ring = chash.serialize();
    //var chash2 = fash.deserialize({
        //log: LOG,
        //algorithm: 'sha256',
        //algorithmMax: fash.SHA_256_MAX,
        //topology: chash.ring_
    //});

    //t.ok(chash2);
    //t.equal(chash2.ring_.length, chash.ring_.length, 'ring sizes should be identical');
    //chash2.ring_.forEach(function(node, index) {
        //var node2 = chash2.ring_[index];
        //t.ok(node._hashspace.eq(node2._hashspace));
        //t.equal(node.hashspace, node2.hashspace);
        //t.equal(node.node, node2.node);
        //t.equal(node.vnode, node2.vnode);
    //});
    //for (var i = 0; i < numberOfKeys; i++) {
        //var random = Math.random().toString(33);
        //var key = random.substring(Math.floor(Math.random() * random.length));
        //var node1 = chash.getNode(key);
        //var node2 = chash2.getNode(key);
        //t.equal(node1.pnode, node2.pnode);
        //t.equal(node1.vnode, node2.vnode);
    //}

    //// add a pnode to both chashes
    //var vnodes = [0, 1, 2, 3, 4];
    //chash.remapNode('F', vnodes);
    //chash2.remapNode('F', vnodes);

    //t.equal(chash2.ring_.length, chash.ring_.length, 'ring sizes should be identical');
    //chash2.ring_.forEach(function(node, index) {
        //var node2 = chash2.ring_[index];
        //t.ok(node._hashspace.eq(node2._hashspace));
        //t.equal(node.hashspace, node2.hashspace);
        //t.equal(node.node, node2.node);
        //t.equal(node.vnode, node2.vnode);
    //});
    //for (var i = 0; i < numberOfKeys; i++) {
        //var random = Math.random().toString(33);
        //var key = random.substring(Math.floor(Math.random() * random.length));
        //var node1 = chash.getNode(key);
        //var node2 = chash2.getNode(key);
        //t.equal(node1.pnode, node2.pnode);
        //t.equal(node1.vnode, node2.vnode);
    //}

    //// let's remove B
    //vnodes = chash.getVnodes('B');
    //// and assign B's nodes to A
    //chash.remapNode('A', Object.keys(vnodes));
    //var vnodes2 = chash2.getVnodes('B');
    //chash2.remapNode('A', Object.keys(vnodes2));
    //chash.removeNode('B', function(err) {
        //t.notOk(err);
        //chash2.removeNode('B', function(err) {
            //t.notOk(err);
            //t.equal(chash2.ring_.length, chash.ring_.length, 'ring sizes should be identical');
            //chash.ring_.forEach(function(node, index) {
                //var node2 = chash2.ring_[index];
                //t.ok(node._hashspace.eq(node2._hashspace));
                //t.equal(node.hashspace, node2.hashspace);
                //t.equal(node.pnode, node2.pnode);
                //t.equal(node.vnode, node2.vnode);
            //});
            //for (var i = 0; i < numberOfKeys; i++) {
                //var random = Math.random().toString(33);
                //var key = random.substring(Math.floor(Math.random() * random.length));
                //var node1 = chash.getNode(key);
                //var node2 = chash2.getNode(key);
                //t.equal(node1.pnode, node2.pnode);
                //t.equal(node1.vnode, node2.vnode);
            //}

            //t.end();
        //});
    //});
//});


//test('hashing the same key', function(t) {
    //var chash = fash.create({
        //log: LOG,
        //algorithm: 'sha256',
        //algorithmMax: fash.SHA_256_MAX,
        //vnodes: numberOfVnodes,
        //pnodes: ['A', 'B', 'C', 'D', 'E'],
        //random: true
    //});
    //for (var i = 0; i < 10; i++) {
        //var random = Math.random().toString(33);
        //var key = random.substring(Math.floor(Math.random() * random.length));
        //var node1 = chash.getNode(key);
        //var node2 = chash.getNode(key);
        //t.equal(node1.pnode, node2.pnode);
        //t.equal(node1.vnode, node2.vnode);
    //}

    //t.end();
//});

//test('collision', function(t) {
    //var caught;
    //try {
        //var chash = fash.create({
            //log: LOG,
            //algorithm: 'sha256',
            //algorithmMax: fash.SHA_256_MAX,
            //pnodes: ['a', 'a'],
            //vnodes: numberOfVnodes,
            //random: true
        //});
    //} catch (e) {
        //caught = true;
    //}
    //t.ok(caught, 'collision of pnodes should throw');
    //t.end();
//});

tap.tearDown(function() {
    process.exit(tap.output.results.fail);
});

/// Private heleprs
var _verifyRing = function _verifyRing(chash, t, cb) {
    t.equal(chash.ring_.length, numberOfVnodes);
    // assert that each node appears once and only once
    var map = {};
    chash.ring_.forEach(function(node) {
        var key = node.hashspace;
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

            var index = parseInt(chash.findVnode(hash), 10);
            var nextNode;
            // if we are at the last vnode, then skip checking for nextNode since there isn't one
            if (index < (numberOfVnodes - 1)) {
                nextNode = bignum(chash.ring_[index + 1].hashspace, 16);
            }

            var currNode = chash.ring_[index].hashspace;
            currNode = bignum(currNode, 16).toString(16);
            // assert hash is in bewtween index + 1 and index
            t.ok(hash.ge(currNode), 'hash ' + bignum(hash, 10).toString(16) +
                 ' should be >= than \n' + currNode.toString(16));
            if (index < (numberOfVnodes - 1)) {
                t.ok(hash.lt(nextNode), 'hash ' + hash + ' should be < than ' +
                    nextNode + ' index is ' + index + ' node ' + util.inspect(node));
            }

            t.ok(node);
            // assert node returned by getNode is the same as what we've calculated
            t.equal(node.pnode, chash.ring_[index].pnode, 'pnodes should match');
            t.equal(node.vnode, chash.ring_[index].vnode, 'vnodes should match');
    }

    return cb();
};

