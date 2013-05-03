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
    level: process.env.LOG_LEVEL || 'warn'
});
var numberOfKeys = process.env.NUMBER_OF_KEYS || 10;
var numberOfVnodes = process.env.NUMBER_OF_VNODES || 100;
var numberOfPnodes = process.env.NUMBER_OF_VNODES || 10;
var PNODES = new Array(numberOfPnodes);

test('before test', function(t) {
    for (var i = 0; i < numberOfPnodes; i++) {
        PNODES[i] = Math.random().toString(33).substr(2, 10);
    }

    PNODES.sort();
    t.end();
});

test('new ring', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: PNODES,
        vnodes: numberOfVnodes,
    });

    _verifyRing(chash, t, function() {
        t.end();
    });
});

test('remapOnePnodeToAnother', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: PNODES,
        vnodes: numberOfVnodes,
    });

    var beforeRing = JSON.parse(chash.serialize()).pnodeToVnodeMap;
    // get vnodes from A
    var aVnodes = chash.getVnodes(PNODES[0]);

    // get vnodes from B for later
    var originalBVnodes = chash.getVnodes(PNODES[1]).slice(0);
    // remap all to B
    chash.remapVnode(PNODES[1], aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var pnode = chash.pnodeToVnodeMap_[PNODES[0]];
        t.ok(pnode, 'A pnode should still exist even if it has no vnodes');
        pnodeKeys = Object.keys(pnode);
        t.ok((pnodeKeys.length === 0),
        'pnode ' + pnodes + ' should not map to any vnodes');

        var remappedVnodes = beforeRing[PNODES[0]];

        Object.keys(remappedVnodes).forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === PNODES[1]),
                 'vnode ' + vnode + ' should belong to B');
        });

        // check B still contains vnodes from its old self.
        originalBVnodes.forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === PNODES[1]),
                 'vnode ' + vnode + ' should still belong to B');
        });

        _verifyRing(chash, t, function() {
            t.end();
        });
    });
});

test('remapSomeVnodeToAnother', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: PNODES,
        vnodes: numberOfVnodes,
    });

    // get vnodes from A
    var aVnodes = chash.getVnodes(PNODES[0]);
    var leftOverAVnodes = aVnodes.slice(0);
    aVnodes = leftOverAVnodes.splice(leftOverAVnodes.length / 2);

    // get vnodes from B for later
    var originalBVnodes = chash.getVnodes(PNODES[1]).slice(0);

    // remap some of A's vnodes to B
    chash.remapVnode(PNODES[1], aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var aVnodesAfter = chash.pnodeToVnodeMap_[PNODES[0]];
        t.ok((Object.keys(aVnodesAfter).length >= 1),
            'A should contain at least one vnode after partial remap');
        var bVnodesAfter = chash.pnodeToVnodeMap_[PNODES[1]];
        t.ok((Object.keys(bVnodesAfter).length >= 1),
            'B should contain at least one vnode after partial remap');
        // check A doesn't contain the remapped nodes
        aVnodes.forEach(function(vnode) {
            t.notOk(aVnodesAfter[vnode],
                    'remapped vnode ' + vnode + ' should not belong to A');
        });

        // check A still contains its non-remapped nodes
        console.log(leftOverAVnodes);
        leftOverAVnodes.forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === PNODES[0]),
                 'vnode ' + vnode + ' should still belong to A');
        });

        // check B contains the remapped nodes
        aVnodes.forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === PNODES[1]),
                 'vnode ' + vnode + ' should belong to B');
        });

        // check B still contains vnodes from its old self.
        originalBVnodes.forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === PNODES[1]),
                 'vnode ' + vnode + ' should still belong to B');
        });

        _verifyRing(chash, t, function() {
            t.end();
        });
    });
});

test('removePnode', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: PNODES,
        vnodes: numberOfVnodes,
    });

    // remap all of A to B
    chash.remapVnode(PNODES[1], chash.getVnodes(PNODES[0]),
                     function(err, ring, pnodes)
    {
        t.ifErr(err);

        chash.removePnode(PNODES[0], function(err, ring) {
            t.ifErr(err);
            t.ok(chash.pnodes_.indexOf(PNODES[0]) === -1,
                 'A should not exist in pnode array');
            t.notOk(chash.pnodeToVnodeMap_[PNODES[0]],
                'A should not exist in pnodeToVnodeMap');
            _verifyRing(chash, t, function() {
                t.end();
            });
        });
    });
});

test('add new pnode', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: PNODES,
        vnodes: numberOfVnodes,
    });

    var beforeRing = JSON.parse(chash.serialize()).pnodeToVnodeMap;
    // get vnodes from A
    var aVnodes = chash.getVnodes(PNODES[0]);

    // remap all to F
    chash.remapVnode('F', aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var pnode = chash.pnodeToVnodeMap_[PNODES[0]];
        t.ok(pnode,
            'pnode A should still exist even if it doesn\'t have vnodes');
        pnodeKeys = Object.keys(pnode);
        t.ok((pnodeKeys.length === 0), 'pnode A should not map to any vnodes');

        var remmappedVnodes = beforeRing[PNODES[0]];

        Object.keys(remmappedVnodes).forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === 'F'),
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
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: PNODES,
        vnodes: numberOfVnodes,
    });

    // get vnodes from A
    var aVnodes = chash.getVnodes(PNODES[0]);
    var leftOverAVnodes = aVnodes.slice(0);
    aVnodes = leftOverAVnodes.splice(leftOverAVnodes.length / 2);

    // remap some of A's vnodes to F
    chash.remapVnode('F', aVnodes, function(err, ring, pnodes) {
        t.ifErr(err);

        var aVnodesAfter = chash.pnodeToVnodeMap_[PNODES[0]];
        t.ok((Object.keys(aVnodesAfter).length >= 1),
            'A should contain at least one vnode after partial remap');
        // check A doesn't contain the remapped nodes
        aVnodes.forEach(function(vnode) {
            t.notOk(aVnodesAfter[vnode],
                    'remapped vnode ' + vnode + ' should not belong to A');
        });

        // check A still contains its non-remapped nodes
        leftOverAVnodes.forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === PNODES[0]),
                 'vnode ' + vnode + ' should still belong to A');
        });

        // check F contains the remapped nodes from A
        aVnodes.forEach(function(vnode) {
            console.log(chash.vnodeToPnodeMap_[vnode]);
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === 'F'),
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
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: PNODES,
        vnodes: numberOfVnodes,
    });

    var chash1 = chash.serialize();

    var chash2 = fash.deserialize({
        log: LOG,
        topology: chash1
    });

    _verifyRing(chash2, t, function() {

        for (var i = 0; i < numberOfKeys; i++) {
            var random = Math.random().toString(33);
            var key = random.substring(Math.floor(Math.random() *
                random.length));
            var node1 = JSON.stringify(chash.getNode(key));
            var node2 = JSON.stringify(chash2.getNode(key));
            t.equal(node1, node2, 'hashed node from serialized and original ' +
                    'ring should be equal');
        }

        t.end();
    });
});

test('hashing the same key', function(t) {
    var chash = fash.create({
        log: LOG,
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: PNODES,
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
        fash.create({
            log: LOG,
            algorithm: fash.ALGORITHMS.SHA256,
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
        var key = random.substring(Math.floor(Math.random() * random.length));
        var node = chash.getNode(key);

        var hash = crypto.createHash('sha256');
        hash.update(key);
        hash = hash.digest('hex');
        hash = bignum(hash, 16);

        var index = parseInt(chash.findVnode(hash), 10);
        var nextNode;
        // if we are at the last vnode, then skip checking for nextNode since
        // there isn't one
        if (index < (numberOfVnodes - 1)) {
            nextNode = bignum(chash.findHashspace(index + 1), 16);
        }

        var currNode = chash.findHashspace(index);
        // assert hash is in between index + 1 and index
        t.ok(hash.ge(currNode), 'hash ' + bignum(hash, 10).toString(16) +
            ' should be >= than \n' + currNode.toString(16));
        if (index < (numberOfVnodes - 1)) {
            t.ok(hash.lt(nextNode), 'hash ' + hash + ' should be < than ' +
                nextNode + ' index is ' + index + ' node ' +
                util.inspect(node));
        }

        t.ok(node);
        // assert node returned by getNode is the same as what we've calculated
        t.equal(node.pnode, chash.vnodeToPnodeMap_[index.toString()].pnode,
        'pnodes should match');
        t.equal(node.vnode, index.toString(), 'vnodes should match');
    }

    return cb();
};
