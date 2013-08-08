var bignum = require('bignum');
var crypto = require('crypto');
var common = require('../lib/common');
var fash = require('../lib');
var Logger = require('bunyan');
var util = require('util');
var uuid = require('node-uuid');

var LOG = new Logger({
    name: 'consistent-hash-test',
    src: true,
    level: process.env.LOG_LEVEL || 'warn'
});
var NUMBER_OF_KEYS = parseInt(process.env.NUMBER_OF_KEYS || 10, 10);
var NUMBER_OF_VNODES = parseInt(process.env.NUMBER_OF_VNODES || 100);
var NUMBER_OF_PNODES = parseInt(process.env.NUMBER_OF_PNODES || 10);
var PNODES = new Array(NUMBER_OF_PNODES);
var ALGORITHM = ['sha256', 'sha1', 'md5'];

exports.beforeTest = function(t) {
    for (var i = 0; i < NUMBER_OF_PNODES; i++) {
        PNODES[i] = Math.random().toString(33).substr(2, 10);
    }

    PNODES.sort();
    t.done();
};

_testAllAlgorithms(function newRing(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    _verifyRing(chash, t, algo, function() {
        t.done();
    });
});

_testAllAlgorithms(function remapOnePnodeToAnother(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var beforeRing = JSON.parse(chash.serialize()).pnodeToVnodeMap;
    // get vnodes from A
    var aVnodes = chash.getVnodes(PNODES[0]);

    // get vnodes from B for later
    var originalBVnodes = chash.getVnodes(PNODES[1]).slice(0);
    // remap all to B
    chash.remapVnode(PNODES[1], aVnodes, function(ring, pnodes) {
        t.ok(ring, 'new ring topology should exist');
        t.ok(pnodes, 'changed pnodes should exist');
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

        _verifyRing(chash, t, algo, function() {
            t.done();
        });
    });
});

_testAllAlgorithms(function remapSomeVnodeToAnother(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    // get vnodes from A
    var aVnodes = chash.getVnodes(PNODES[0]);
    var leftOverAVnodes = aVnodes.slice(0);
    aVnodes = leftOverAVnodes.splice(leftOverAVnodes.length / 2);

    // get vnodes from B for later
    var originalBVnodes = chash.getVnodes(PNODES[1]).slice(0);

    // remap some of A's vnodes to B
    chash.remapVnode(PNODES[1], aVnodes, function(ring, pnodes) {
        t.ok(ring, 'new ring topology should exist');
        t.ok(pnodes, 'changed pnodes should exist');
        var aVnodesAfter = chash.pnodeToVnodeMap_[PNODES[0]];
        t.ok((Object.keys(aVnodesAfter).length >= 1),
            'A should contain at least one vnode after partial remap');
        var bVnodesAfter = chash.pnodeToVnodeMap_[PNODES[1]];
        t.ok((Object.keys(bVnodesAfter).length >= 1),
            'B should contain at least one vnode after partial remap');
        // check A doesn't contain the remapped nodes
        aVnodes.forEach(function(vnode) {
            t.ok(!aVnodesAfter[vnode],
                 'remapped vnode ' + vnode + ' should not belong to A');
        });

        // check A still contains its non-remapped nodes
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

        _verifyRing(chash, t, algo, function() {
            t.done();
        });
    });
});

_testAllAlgorithms(function removePnode(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    // remap all of A to B
    chash.remapVnode(PNODES[1], chash.getVnodes(PNODES[0]),
                     function(ring, pnodes) {
        t.ok(ring, 'new ring topology should exist');
        t.ok(pnodes, 'changed pnodes should exist');
        chash.removePnode(PNODES[0], function(ring, pnodes) {
            t.ok(ring, 'new ring topology should exist');
            t.ok(pnodes, 'changed pnodes should exist');
            t.ok(chash.pnodes_.indexOf(PNODES[0]) === -1,
                 'A should not exist in pnode array');
            t.ok(!chash.pnodeToVnodeMap_[PNODES[0]],
                'A should not exist in pnodeToVnodeMap');
            // other pnodes should still exist
            t.equal(chash.pnodes_.length, PNODES.length - 1,
                'should have 1 less pnode after remove');
            t.equal(Object.keys(chash.pnodeToVnodeMap_).length,
                PNODES.length - 1, 'should have 1 less pnode after remove');
            _verifyRing(chash, t, algo, function() {
                t.done();
            });
        });
    });
});

_testAllAlgorithms(function add_new_pnode(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var beforeRing = JSON.parse(chash.serialize()).pnodeToVnodeMap;
    // get vnodes from A
    var aVnodes = chash.getVnodes(PNODES[0]);

    // remap all to F
    chash.remapVnode('F', aVnodes, function(ring, pnodes) {
        t.ok(ring, 'new ring topology should exist');
        t.ok(pnodes, 'changed pnodes should exist');
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

        _verifyRing(chash, t, algo, function() {
            t.done();
        });
    });
});

_testAllAlgorithms(function add_new_pnode_remap_only_subset_of_old_pnode(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    // get vnodes from A
    var aVnodes = chash.getVnodes(PNODES[0]);
    var leftOverAVnodes = aVnodes.slice(0);
    aVnodes = leftOverAVnodes.splice(leftOverAVnodes.length / 2);

    // remap some of A's vnodes to F
    chash.remapVnode('F', aVnodes, function(ring, pnodes) {
        t.ok(ring, 'new ring topology should exist');
        t.ok(pnodes, 'changed pnodes should exist');
        var aVnodesAfter = chash.pnodeToVnodeMap_[PNODES[0]];
        t.ok((Object.keys(aVnodesAfter).length >= 1),
            'A should contain at least one vnode after partial remap');
        // check A doesn't contain the remapped nodes
        aVnodes.forEach(function(vnode) {
            t.ok(!aVnodesAfter[vnode],
                    'remapped vnode ' + vnode + ' should not belong to A');
        });

        // check A still contains its non-remapped nodes
        leftOverAVnodes.forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === PNODES[0]),
                 'vnode ' + vnode + ' should still belong to A');
        });

        // check F contains the remapped nodes from A
        aVnodes.forEach(function(vnode) {
            t.ok((chash.vnodeToPnodeMap_[vnode].pnode === 'F'),
                 'vnode ' + vnode + ' should belong to F');
        });

        _verifyRing(chash, t, algo, function() {
            t.done();
        });
    });
});

_testAllAlgorithms(function deserialize_hash_ring(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var chash1 = chash.serialize();
    var chash2 = fash.deserialize({
        log: LOG,
        topology: chash1,
        backend: fash.BACKEND.IN_MEMORY
    });

    _verifyRing(chash2, t, algo, function() {
        for (var i = 0; i < NUMBER_OF_KEYS; i++) {
            var random = Math.random().toString(33);
            var key = random.substring(Math.floor(Math.random() *
                random.length));
            var node1 = JSON.stringify(chash.getNode(key));
            var node2 = JSON.stringify(chash2.getNode(key));
            t.equal(node1, node2, 'hashed node from serialized and original ' +
                    'ring should be equal');
        }

        t.done();
    });
});

_testAllAlgorithms(function add_data(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var vnode = parseInt(NUMBER_OF_VNODES * Math.random(), 10);
    chash.addData(vnode, 'foo');
    t.equal(chash.vnodeToPnodeMap_[vnode].data, 'foo',
        'stored data should match put data');
    var pnode = chash.vnodeToPnodeMap_[vnode].pnode;
    t.equal(chash.pnodeToVnodeMap_[pnode][vnode], 'foo',
        'stored data should match put data');
    _verifyRing(chash, t, algo, function() {
        t.done();
    });
});

_testAllAlgorithms(function add_data_remap_vnode_to_different_pnode(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var vnode = parseInt(NUMBER_OF_VNODES * Math.random(), 10);
    var originalPnode = chash.vnodeToPnodeMap_[vnode].pnode;
    var pnode;
    for (var i = 0; i < PNODES.length; i++) {
        if (PNODES[i] !== originalPnode) {
            pnode = PNODES[i];
        }
    }
    chash.addData(vnode, 'foo');

    // remap all to B
    chash.remapVnode(pnode, [vnode], function(ring, pnodes) {
        t.ok(ring, 'new ring topology should exist');
        t.ok(pnodes, 'changed pnodes should exist');
        t.equal(chash.vnodeToPnodeMap_[vnode].data, 'foo',
            'stored data should match put data');
        t.equal(chash.pnodeToVnodeMap_[pnode][vnode], 'foo',
            'stored data should match put data');
        _verifyRing(chash, t, algo, function() {
            t.done();
        });
    });
});

_testAllAlgorithms(function add_data_serialize_deserialize(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var vnode = parseInt(NUMBER_OF_VNODES * Math.random(), 10);
    var pnode = chash.vnodeToPnodeMap_[vnode].pnode;
    chash.addData(vnode, 'foo');

    var chash1 = chash.serialize();
    t.ok(JSON.parse(chash1).version, 'serialized hash should contain version');

    var chash2 = fash.deserialize({
        log: LOG,
        topology: chash1,
        backend: fash.BACKEND.IN_MEMORY
    });
    t.equal(chash2.vnodeToPnodeMap_[vnode].data, 'foo',
        'stored data should match put data in serialized hash');
    t.equal(chash2.pnodeToVnodeMap_[pnode][vnode], 'foo',
        'stored data should match put data in serialized hash');

    _verifyRing(chash2, t, algo, function() {
        for (var i = 0; i < NUMBER_OF_KEYS; i++) {
            var random = Math.random().toString(33);
            var key = random.substring(Math.floor(Math.random() *
                random.length));
            var node1 = JSON.stringify(chash.getNode(key));
            var node2 = JSON.stringify(chash2.getNode(key));
            t.equal(node1, node2, 'hashed node from serialized and original ' +
                    'ring should be equal');
        }
        t.done();
    });
});

_testAllAlgorithms(function add_data_overwrite(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var vnode = parseInt(NUMBER_OF_VNODES * Math.random(), 10);
    chash.addData(vnode, 'foo');
    chash.addData(vnode, 'bar');
    t.equal(chash.vnodeToPnodeMap_[vnode].data, 'bar',
        'replaced data should match put data');
    var pnode = chash.vnodeToPnodeMap_[vnode].pnode;
    t.equal(chash.pnodeToVnodeMap_[pnode][vnode], 'bar',
        'replaced data should match put data');
    _verifyRing(chash, t, algo, function() {
        t.done();
    });
});

_testAllAlgorithms(function add_data_overwrite_with_null(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var vnode = parseInt(NUMBER_OF_VNODES * Math.random(), 10);
    chash.addData(vnode, 'foo');
    chash.addData(vnode, null);
    t.equal(chash.vnodeToPnodeMap_[vnode].data, null,
        'deleted data should be null');
    var pnode = chash.vnodeToPnodeMap_[vnode].pnode;
    t.equal(chash.pnodeToVnodeMap_[pnode][vnode], null,
        'deleted data should be null');
    _verifyRing(chash, t, algo, function() {
        t.done();
    });
});

_testAllAlgorithms(function hashing_the_same_key(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
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

    t.done();
});

/// Negative tests

_testAllAlgorithms(function deserialize_newer_version(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var chash1 = chash.serialize();
    chash1 = JSON.parse(chash1);
    chash1.version = '123123123123123123123123123123123.123123123123.12312321';
    chash1 = JSON.stringify(chash1);

    var caught;
    try {
        var chash2 = fash.deserialize({
            log: LOG,
            topology: chash1,
            backend: fash.BACKEND.IN_MEMORY
        });
    } catch (e) {
        caught = true;
    }

    t.ok(caught, 'deserializing newer version should throw')
    t.done();
});

_testAllAlgorithms(function collision(algo, t) {
    var caught;
    try {
        fash.create({
            log: LOG,
            algorithm: algo,
            pnodes: ['a', 'a'],
            vnodes: NUMBER_OF_VNODES,
            backend: fash.BACKEND.IN_MEMORY
        });
    } catch (e) {
        caught = true;
    }
    t.ok(caught, 'collision of pnodes should throw');
    t.done();
});

_testAllAlgorithms(function remap_non_existent_vnodes(algo, t) {
    var caught;
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    try {
        chash.remapVnode(PNODES[0], [NUMBER_OF_VNODES + 100]);
    } catch (e) {
        caught = true;
    }

    t.ok(caught, 'remapping non-existent vnodes should throw');
    t.done();
});

_testAllAlgorithms(function remap_vnode_to_the_same_pnode(algo, t) {
    var caught;
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });
    var vnode = parseInt(NUMBER_OF_VNODES * Math.random(), 10);
    var originalPnode = chash.vnodeToPnodeMap_[vnode].pnode;
    var pnode;
    for (var i = 0; i < PNODES.length; i++) {
        if (PNODES[i] !== originalPnode) {
            pnode = PNODES[i];
        }
    }

    try {
        chash.remapVnode(pnode, [vnode, vnode]);
    } catch (e) {
        caught = true;
    }

    t.ok(caught, 'remapping vnodes to the original pnode should throw');
    t.done();
});

_testAllAlgorithms(function remap_the_same_vnode_more_than_once(algo, t) {
    var caught;
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });
    var vnode = parseInt(NUMBER_OF_VNODES * Math.random(), 10);
    var originalPnode = chash.vnodeToPnodeMap_[vnode].pnode;

    try {
        chash.remapVnode(originalPnode, vnode);
    } catch (e) {
        caught = true;
    }

    t.ok(caught, 'remapping the same vnode more than once should throw');
    t.done();
});

_testAllAlgorithms(function remove_non_existent_pnode_should_throw(algo, t) {
    var caught;
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });
    try {
        chash.removePnode(uuid.v4());
    } catch(e) {
        caught = true;
    }
    t.ok(caught, 'removing non-existent pnode should throw');
    t.done();
});

_testAllAlgorithms(function remove_pnode_which_has_vnode_should_throw(algo, t) {
    var caught;
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });
    try {
        chash.removePnode(PNODES[0]);
    } catch(e) {
        caught = true;
    }
    t.ok(caught, 'removing pnode that still has vnodes should throw');
    t.done();
});

_testAllAlgorithms(function node_fash_8_null_out_vnode(algo, t) {
    var chash = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.IN_MEMORY
    });

    var vnode = Math.round(Math.random() * NUMBER_OF_VNODES);
    var pnode = chash.vnodeToPnodeMap_[vnode].pnode;
    chash.addData(vnode, undefined, function(topology) {
        var vnodeStr = vnode.toString();
        t.ok(topology);
        t.ok(Object.keys(topology.pnodeToVnodeMap[pnode]).indexOf(vnode.toString()) !== -1,
             'vnode ' + vnode + ' does not exist in topology');
        t.done();
    });
});

// Private heleprs
function _verifyRing(chash, t, algo, cb) {
    // assert that each node appears once and only once
    var map = {};
    var vnodes = Object.keys(chash.vnodeToPnodeMap_);
    t.equal(vnodes.length, NUMBER_OF_VNODES);
    vnodes.forEach(function(key) {
        t.ok(!map[key], 'hashspace should not exist');
        map[key] = 1;
    });
    t.equal(chash.pnodes_.length, Object.keys(chash.pnodeToVnodeMap_).length,
            'pnodes_ and pvmap must have same length');

    // randomly pick some keys and see if they hash to their expected values
    for (var i = 0; i < NUMBER_OF_KEYS; i++) {
        var random = Math.random().toString(33);
        var key = random.substring(Math.floor(Math.random() * random.length));
        var node = chash.getNode(key);

        var hash = crypto.createHash(algo);
        hash.update(key);
        hash = hash.digest('hex');
        hash = bignum(hash, 16);

        var index = parseInt(chash.findVnode(hash), 10);
        var nextNode;
        // if we are at the last vnode, then skip checking for nextNode since
        // there isn't one
        if (index < (NUMBER_OF_VNODES - 1)) {
            nextNode = bignum(common.findHashspace({
                vnode: index + 1,
                log: chash.log,
                vnodeHashInterval: chash.VNODE_HASH_INTERVAL
            }), 16);
        }

        var currNode = common.findHashspace({
            vnode: index,
            log: chash.log,
            vnodeHashInterval: chash.VNODE_HASH_INTERVAL
        });
        // assert hash is in between index + 1 and index
        t.ok(hash.ge(currNode), 'hash ' + bignum(hash, 10).toString(16) +
            ' should be >= than \n' + currNode.toString(16));
        if (index < (NUMBER_OF_VNODES - 1)) {
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

function _testAllAlgorithms(test) {
    ALGORITHM.forEach(function(algo) {
        exports[test.name + algo] = test.bind(null, algo);
    });
};
