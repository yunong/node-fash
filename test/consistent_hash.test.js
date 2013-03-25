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
var numberOfKeys = 10;
var numberOfReplicas = 2;
//var numberOfVnodes = 10000;
var numberOfVnodes = 10;

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

    var beforeRing = JSON.parse(chash.serialize()).pnodeToVnodeMap;
    // get vnodes from A
    var aVnodes = chash.getVnodes('A');

    // remap all to B
    chash.remapVnode('B', aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var pnode = chash.pnodeToVnodeMap_['A'];
        if (pnode) {

            pnodeKeys = Object.keys(pnode);
            t.ok((pnodeKeys.length === 0),
            'pnode A should not map to any vnodes');
        }

        var remmappedVnodes = beforeRing['A'];

        Object.keys(remmappedVnodes).forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode] === 'B'),
                 'vnode ' + vnode + ' should belong to B');
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

    var beforeRing = JSON.parse(chash.serialize()).pnodeToVnodeMap;
    // get vnodes from A
    var aVnodes = chash.getVnodes('A');
    aVnodes = aVnodes.slice(aVnodes.length / 2);

    // remap some of A's vnodes to B
    chash.remapVnode('B', aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var aVnodesAfter = chash.pnodeToVnodeMap_['A'];
        aVnodes.forEach(function(vnode) {
            t.notOk(aVnodesAfter[vnode],
                    'remapped vnode ' + vnode + ' should not belong to A');
        });

        aVnodes.forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode] === 'B'),
                 'vnode ' + vnode + ' should belong to B');
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

    var beforeRing = JSON.parse(chash.serialize()).pnodeToVnodeMap;
    // get vnodes from A
    var aVnodes = chash.getVnodes('A');

    // remap all to B
    chash.remapVnode('F', aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var pnode = chash.pnodeToVnodeMap_['A'];
        if (pnode) {

            pnodeKeys = Object.keys(pnode);
            t.ok((pnodeKeys.length === 0),
            'pnode A should not map to any vnodes');
        }

        var remmappedVnodes = beforeRing['A'];

        Object.keys(remmappedVnodes).forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode] === 'F'),
                 'vnode ' + vnode + ' should belong to F');
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

        var aVnodesAfter = chash.pnodeToVnodeMap_['A'];
        aVnodes.forEach(function(vnode) {
            t.notOk(aVnodesAfter[vnode], 'remapped vnode ' + vnode + ' should not belong to A');
        });

        aVnodes.forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode] === 'F'),
                 'vnode ' + vnode + ' should belong to F');
        });

        _verifyRing(chash, t, function() {
            t.end();
        });
    });
});

test('deserialize hash ring', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: 'sha256',
        algorithmMax: fash.SHA_256_MAX,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: numberOfVnodes,
    });

    var chash1 = chash.serialize();

    var chash2 = fash.deserialize({
        log: LOG,
        algorithm: 'sha256',
        algorithmMax: fash.SHA_256_MAX,
        topology: chash1
    });

    _verifyRing(chash2, t, function() {
        t.end();
    });
});

test('hashing the same key', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: 'sha256',
        algorithmMax: fash.SHA_256_MAX,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: numberOfVnodes,
    });
    for (var i = 0; i < 10; i++) {
        var random = Math.random().toString(33);
        var key = random.substring(Math.floor(Math.random() * random.length));
        var node1 = chash.getNode(key);
        var node2 = chash.getNode(key);
        t.equal(node1.pnode, node2.pnode,
                'hashing the same key twice should return the same pnode');
        t.equal(node1.vnode, node2.vnode,
                'hashing the same key twice should return the same vnode');
    }

    t.end();
});

test('collision', function(t) {
    var caught;
    try {
        var chash = fash.create({
            log: LOG,
            algorithm: 'sha256',
            algorithmMax: fash.SHA_256_MAX,
            pnodes: ['a', 'a'],
            vnodes: numberOfVnodes
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

/// Private heleprs
var _verifyRing = function _verifyRing(chash, t, cb) {
    // assert that each node appears once and only once
    var map = {};
    var vnodes = Object.keys(chash.vnodeToPnodeMap_);
    t.equal(vnodes.length, numberOfVnodes);
    vnodes.forEach(function(key) {
        t.notOk(map[key], 'hashspace should not exist');
        map[key] = 1;
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
                nextNode = bignum(chash.findHashspace(index + 1), 16);
            }

            var currNode = chash.findHashspace(index);
            // assert hash is in between index + 1 and index
            t.ok(hash.ge(currNode), 'hash ' + bignum(hash, 10).toString(16) +
                 ' should be >= than \n' + currNode.toString(16));
            if (index < (numberOfVnodes - 1)) {
                t.ok(hash.lt(nextNode), 'hash ' + hash + ' should be < than ' +
                    nextNode + ' index is ' + index + ' node ' + util.inspect(node));
            }

            t.ok(node);
            // assert node returned by getNode is the same as what we've calculated
            t.equal(node.pnode, chash.vnodeToPnodeMap_[index.toString()], 'pnodes should match');
            t.equal(node.vnode, index.toString(), 'vnodes should match');
    }

    return cb();
};

