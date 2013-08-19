var bignum = require('bignum');
var crypto = require('crypto');
var common = require('../lib/common');
var exec = require('child_process').exec;
var fash = require('../lib');
var leveldb = require('../lib/backend/leveldb');
var Logger = require('bunyan');
var lodash = require('lodash');
var once = require('once');
var sprintf = require('util').format;
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');

var LOG = new Logger({
    name: 'consistent-hash-test',
    src: true,
    level: process.env.LOG_LEVEL || 'warn'
});
var FASH_CLI_PATH = process.env.FASH_CLI_PATH || './bin/fash.js';
var NUMBER_OF_KEYS = parseInt(process.env.NUMBER_OF_KEYS || 1000, 10);
var NUMBER_OF_VNODES = parseInt(process.env.NUMBER_OF_VNODES || 100);
var NUMBER_OF_PNODES = parseInt(process.env.NUMBER_OF_PNODES || 10);
var PNODES = new Array(NUMBER_OF_PNODES);
var PNODE_STRING = '\'';
var ALGORITHM = ['sha256', 'sha1', 'md5'];

exports.beforeTest = function(t) {
    for (var i = 0; i < NUMBER_OF_PNODES; i++) {
        PNODES[i] = 'pnode' + i;
        if (i === 0) {
            PNODE_STRING += PNODES[i];
        } else {
            PNODE_STRING += ' ' + PNODES[i];
        }
    }
    PNODE_STRING += '\'';

    PNODES.sort();
    t.done();
};

_testAllConstructors(function newRing(algo, constructor, t) {
    constructor(algo, function(err, hLevel, hInMem) {
        if (err) {
            t.fail(err);
            t.done();
        }
        _verifyRing(hLevel, hInMem, t, algo, function() {
            t.done();
        });
    });
});

_testAllConstructors(function remapOnePnodeToAnother(algo, constructor, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function getVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[0], function(err, vnodes) {
                _.vnodes = vnodes;
                return cb(err);
            });
        },
        function remap(_, cb) {
            _.hInMem.remapVnode(PNODES[1], _.vnodes);
            var count = 0;
            // remap to the second pnode
            _.hLevel.remapVnode(PNODES[1], _.vnodes[count++], remapCb);

            // ensure serial invocations of remap
            function remapCb(err) {
                if (err) {
                    return cb(err);
                }
                if (count === _.vnodes.length) {
                    return cb();
                }
                _.hLevel.remapVnode(PNODES[1], _.vnodes[count++], remapCb);
            };
        },
        function assertVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[1], function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(PNODES[1]);
                t.ok(lodash.isEqual(vnodes.sort(), inMemVnodes.sort()),
                              'level vnodes should equal in mem vnodes');
                return cb();
            });
        },
        function verify(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        }
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function remapSomeVnodeToAnother(algo, constructor, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function getVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[0], function(err, vnodes) {
                _.vnodes = vnodes.splice(vnodes.length / 2);
                return cb(err);
            });
        },
        function remap(_, cb) {
            _.hInMem.remapVnode(PNODES[1], _.vnodes);
            var count = 0;
            // remap to the second pnode
            _.hLevel.remapVnode(PNODES[1], _.vnodes[count++], remapCb);

            // ensure serial invocations of remap
            function remapCb(err) {
                if (err) {
                    return cb(err);
                }
                if (count === _.vnodes.length) {
                    return cb();
                }
                _.hLevel.remapVnode(PNODES[1], _.vnodes[count++], remapCb);
            };
        },
        function assertVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[1], function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(PNODES[1]);
                t.ok(lodash.isEqual(vnodes.sort(), inMemVnodes.sort()),
                              'level vnodes should equal in mem vnodes');
                return cb();
            });
        },
        function verify(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        }
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function removePnode(algo, constructor, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function getVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[0], function(err, vnodes) {
                _.vnodes = vnodes;
                return cb(err);
            });
        },
        function remap(_, cb) {
            _.hInMem.remapVnode(PNODES[1], _.vnodes);
            var count = 0;
            // remap to the second pnode
            _.hLevel.remapVnode(PNODES[1], _.vnodes[count++], remapCb);

            // ensure serial invocations of remap
            function remapCb(err) {
                if (err) {
                    return cb(err);
                }
                if (count === _.vnodes.length) {
                    return cb();
                }
                _.hLevel.remapVnode(PNODES[1], _.vnodes[count++], remapCb);
            };
        },
        function assertVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[1], function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(PNODES[1]);
                t.ok(lodash.isEqual(vnodes.sort(), inMemVnodes.sort()),
                              'level vnodes should equal in mem vnodes');
                return cb();
            });
        },
        function removePnode(_, cb) {
            _.hLevel.removePnode(PNODES[0], function(err) {
                if (err) {
                    return cb(err);
                }
                _.hInMem.removePnode(PNODES[0]);
                return cb();
            });
        },
        function verify(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
        // removing the pnode again should throw
        function removePnodeAgain(_, cb) {
            _.hLevel.removePnode(PNODES[0], function(err) {
                if (!err) {
                    return cb(new Error('removing pnode again should throw'));
                } else {
                    return cb();
                }
            });
        }
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function addNewPnode(algo, constructor, t) {
    var newPnode = 'yunong';
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function getVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[0], function(err, vnodes) {
                _.vnodes = vnodes;
                return cb(err);
            });
        },
        function remap(_, cb) {
            _.hInMem.remapVnode(newPnode, _.vnodes);
            var count = 0;
            // remap to the second pnode
            _.hLevel.remapVnode(newPnode, _.vnodes[count++], remapCb);

            // ensure serial invocations of remap
            function remapCb(err) {
                if (err) {
                    return cb(err);
                }
                if (count === _.vnodes.length) {
                    return cb();
                }
                _.hLevel.remapVnode(newPnode, _.vnodes[count++], remapCb);
            };
        },
        function assertVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[1], function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(PNODES[1]);
                t.ok(lodash.isEqual(vnodes.sort(), inMemVnodes.sort()),
                              'level vnodes should equal in mem vnodes');
                return cb();
            });
        },
        function verify(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
        function removePnode(_, cb) {
            _.hLevel.removePnode(PNODES[0], function(err) {
                if (err) {
                    return cb(err);
                }
                _.hInMem.removePnode(PNODES[0]);
                return cb();
            });
        },
        function verify2(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
        // removing the pnode again should throw
        function removePnodeAgain(_, cb) {
            _.hLevel.removePnode(PNODES[0], function(err) {
                if (!err) {
                    return cb(new Error('removing pnode again should throw'));
                } else {
                    return cb();
                }
            });
        }
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function addNewPnodeRemapOnlySubsetOfOldPnode(algo,
                                                                   constructor,
                                                                   t)
{
    var newPnode = 'yunong';
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function getVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[0], function(err, vnodes) {
                _.vnodes = vnodes.splice(vnodes.length / 2);
                return cb(err);
            });
        },
        function remap(_, cb) {
            _.hInMem.remapVnode(newPnode, _.vnodes);
            var count = 0;
            // remap to the second pnode
            _.hLevel.remapVnode(newPnode, _.vnodes[count++], remapCb);

            // ensure serial invocations of remap
            function remapCb(err) {
                if (err) {
                    return cb(err);
                }
                if (count === _.vnodes.length) {
                    return cb();
                }
                _.hLevel.remapVnode(newPnode, _.vnodes[count++], remapCb);
            };
        },
        function assertVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[1], function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(PNODES[1]);
                t.ok(lodash.isEqual(vnodes.sort(), inMemVnodes.sort()),
                              'level vnodes should equal in mem vnodes');
                return cb();
            });
        },
        function verify(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
        // removing the oldpnode should throw
        function removePnode(_, cb) {
            _.hLevel.removePnode(PNODES[0], function(err) {
                if (!err) {
                    return cb(new Error('removing old pnode should throw'));
                } else {
                    return cb();
                }
            });
        },
        function verify2(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function addData(algo, constructor, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function pickVnode(_, cb) {
            _.key = uuid.v4();
            _.hLevel.getNode(_.key, function(err, node) {
                _.node = node;
                return cb(err);
            });
        },
        function addData(_, cb) {
            _.hLevel.addData(_.node.vnode, 'foo', cb);
            _.hInMem.addData(_.node.vnode, 'foo');
        },
        function getData(_, cb) {
            _.hLevel.getNode(_.key, function(err, node) {
                if (err) {
                    return cb(err);
                }
                t.strictEqual(node.data, 'foo',
                              'stored data should match put data');
                return cb();
            });
        },
        function verify(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function addDataRemapVnodeToDifferentPnode(algo,
                                                                constructor,
                                                                t)
{
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function pickVnode(_, cb) {
            _.key = uuid.v4();
            _.hLevel.getNode(_.key, function(err, node) {
                _.node = node;
                return cb(err);
            });
        },
        function pickNewPnode(_, cb) {
            for (var i = 0; i < PNODES.length; i++) {
                pnode = PNODES[i];
                if (pnode !== _.node.pnode) {
                    _.newPnode = pnode;
                    cb();
                    break;
                }
            }
        },
        function addData(_, cb) {
            _.hLevel.addData(_.node.vnode, 'foo', cb);
            _.hInMem.addData(_.node.vnode, 'foo');
        },
        function getData(_, cb) {
            _.hLevel.getNode(_.key, function(err, node) {
                if (err) {
                    return cb(err);
                }
                t.strictEqual(node.data, 'foo',
                              'stored data should match put data');
                return cb();
            });
        },
        function verify(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
        function remap(_, cb) {
            _.hInMem.remapVnode(_.newPnode, _.node.vnode);
            _.hLevel.remapVnode(_.newPnode, _.node.vnode, cb);
        },
        function assertVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[1], function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(PNODES[1]);
                t.ok(lodash.isEqual(vnodes.sort(), inMemVnodes.sort()),
                              'level vnodes should equal in mem vnodes');
                return cb();
            });
        },
        function verify2(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
        function getData2(_, cb) {
            _.hLevel.getNode(_.key, function(err, node) {
                if (err) {
                    return cb(err);
                }
                t.strictEqual(node.data, 'foo',
                              'stored data should match put data');
                t.strictEqual(node.vnode, _.node.vnode,
                              'vnode should be the same');
                return cb();
            });
        },
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function addDataRemapVnodeToNewPnode(algo, constructor,
                                                          t)
{
    var newPnode = 'yunong';
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function pickVnode(_, cb) {
            _.key = uuid.v4();
            _.hLevel.getNode(_.key, function(err, node) {
                _.node = node;
                return cb(err);
            });
        },
        function addData(_, cb) {
            _.hLevel.addData(_.node.vnode, 'foo', cb);
            _.hInMem.addData(_.node.vnode, 'foo');
        },
        function getData(_, cb) {
            _.hLevel.getNode(_.key, function(err, node) {
                if (err) {
                    return cb(err);
                }
                t.strictEqual(node.data, 'foo',
                              'stored data should match put data');
                return cb();
            });
        },
        function verify(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
        function remap(_, cb) {
            _.hInMem.remapVnode(newPnode, _.node.vnode);
            _.hLevel.remapVnode(newPnode, _.node.vnode, cb);
        },
        function assertVnodes(_, cb) {
            _.hLevel.getVnodes(PNODES[1], function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(PNODES[1]);
                t.ok(lodash.isEqual(vnodes.sort(), inMemVnodes.sort()),
                              'level vnodes should equal in mem vnodes');
                return cb();
            });
        },
        function verify2(_, cb) {
            _verifyRing(_.hLevel, _.hInMem, t, algo, cb);
        },
        function getData2(_, cb) {
            _.hLevel.getNode(_.key, function(err, node) {
                if (err) {
                    return cb(err);
                }
                t.strictEqual(node.data, 'foo',
                              'stored data should match put data');
                t.strictEqual(node.vnode, _.node.vnode,
                              'vnode should be the same');
                return cb();
            });
        },
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function serialize(algo, constructor, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function serialize(_, cb) {
            _.hLevel.serialize(function(err, topology) {
                if (err) {
                    return cb(err);
                }
                _.topology = topology;
                return cb();
            });
        },
        function compareWithInMem(_, cb) {
            var inMemTopology = _.hInMem.serialize();
            t.ok(lodash.isEqual(JSON.parse(_.topology),
                                JSON.parse(inMemTopology)),
                    'topology should match in mem test version');
            return cb();
        }
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

// negative tests
_testAllAlgorithms(function collision(algo, t) {
    fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: ['a', 'a'],
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.LEVEL_DB,
        location: '/tmp/' + uuid.v4()
    }, function(err) {
        t.ok(err, 'identical pnodes should throw');
        t.done();
    });
});

_testAllConstructors(function remapNonExistentVnodes(algo, constructor, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function remap(_, cb) {
            _.hLevel.remapVnode('yunong', NUMBER_OF_VNODES + 10, function(err) {
                t.ok(err, 'remapping non-existent vnode should throw');
                return cb();
            });
        }
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function remapVnodeToTheSamePnode(algo, constructor, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function pickVnode(_, cb) {
            _.key = uuid.v4();
            _.hLevel.getNode(_.key, function(err, node) {
                _.node = node;
                return cb(err);
            });
        },
        function remap(_, cb) {
            _.hLevel.remapVnode(_.node.pnode, _.node.vnode, function(err) {
                t.ok(err, 'remapping vnode to the same pnode should throw');
                return cb();
            });
        },
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function removeNonExistentPnode(algo, constructor, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function remap(_, cb) {
            _.hLevel.removePnode('yunong', function(err) {
                t.ok(err, 'removing non-existent pnode should throw');
                return cb();
            });
        },
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllConstructors(function removeNonEmptyPnode(algo, constructor, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            constructor(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
            });
        },
        function pickVnode(_, cb) {
            _.key = uuid.v4();
            _.hLevel.getNode(_.key, function(err, node) {
                _.node = node;
                return cb(err);
            });
        },
        function remap(_, cb) {
            _.hLevel.removePnode(_.node.pnode, function(err) {
                t.ok(err, 'removing non-empty pnode should throw');
                return cb();
            });
        },
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

// private helpers
function _verifyRing(h1, h2, t, algo, cb) {
    // XXX check validity of keys in leveldb.
    vasync.pipeline({funcs: [
        function checkVnodes(_, _cb) {
            h1.db_.get(leveldb.LKEY_VNODE_COUNT, function(err, vnodeCount) {
                if (err) {
                    return _cb(new verror.VError(err));
                }
                t.ok(vnodeCount, 'VNODE_COUNT does not exist in leveldb');
                t.equal(vnodeCount, NUMBER_OF_VNODES, 'vnode not equal');
                return _cb();
            });
        },
        function checkComplete(_, _cb) {
            h1.db_.get(leveldb.LKEY_COMPLETE, function(err, value) {
                if (err) {
                    err = new verror.VError(err);
                }
                return _cb(err);
            });
        },
        function checkVersion(_, _cb) {
            _cb = once(_cb);
            h1.db_.get(leveldb.LKEY_VERSION, function(err, version) {
                if (err) {
                    return _cb(new verror.VError(err));
                }
                try {
                    fash.assertVersion(version);
                    return _cb();
                } catch(e) {
                    return _cb(new verror.VError(e));
                }
            });
        },
        function checkSlashVnodeSlashN(_, _cb) {
            // spot check /vnode/largestVnode
            h1.db_.get(sprintf(leveldb.LKEY_VNODE_V, NUMBER_OF_VNODES - 1),
                       function(err, pnode)
            {
                if (err) {
                    err = new verror.VError(err);
                }
                t.ok(pnode, 'pnode does not exist in leveldb');
                return _cb(err);
            });
        },
        function getAlgorithm(_, _cb){
            h1.db_.get(leveldb.LKEY_ALGORITHM, function(err, algorithm) {
                if (err) {
                    return _cb(new verror.VError(err));
                }
                t.ok(algorithm, 'ALGORITHM not exist in leveldb');
                // algorithm is asserted by next function

                return _cb();
            });
        },
        function verifyAgainstInMem(_, cb) {
            // compare hashing of in_memory hash to the leveldb hash
            var count = 0;
            for (var i = 0; i < NUMBER_OF_KEYS; i++) {
                var random = Math.random().toString(33);
                var key = random.substring(Math.floor(Math.random() * random.length));
                h1.getNode(key, (function(k, err, node1) {
                    var node2 = h2.getNode(k);
                    LOG.debug({
                        err: err,
                        key: k,
                        node1: node1,
                        node2: node2,
                        count: count
                    }, 'returned from getNode');

                    if (err) {
                        return cb(err);
                    }
                    t.strictEqual(node1.pnode, node2.pnode, 'hashed node ' +
                                  util.inspect(node1) +
                                  ' does not match test in-mem hash' +
                                  util.inspect(node2));
                    t.strictEqual(node1.vnode, node2.vnode, 'hashed node ' +
                                  util.inspect(node1) +
                                  ' does not match test in-mem hash' +
                                  util.inspect(node2));
                    t.strictEqual(node1.data, node2.data, 'hashed node ' +
                                  util.inspect(node1) +
                                  ' does not match test in-mem hash' +
                                  util.inspect(node2));
                    if (++count === (NUMBER_OF_KEYS - 1)) {
                        return cb();
                    }
                }).bind(this, key));
            }
        }
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
}

function _newRing(algo, cb) {
    var h1 = fash.create({
        log: LOG,
        algorithm: algo,
        pnodes: PNODES,
        vnodes: NUMBER_OF_VNODES,
        backend: fash.BACKEND.LEVEL_DB,
        location: '/tmp/' + uuid.v4()
    }, function(err) {
        if (err) {
            return cb(err);
        }
        var h2 = fash.create({
            log: new Logger({
                name: 'test-hash',
                src: true,
                level: 'fatal'
            }),
            algorithm: algo,
            pnodes: PNODES,
            vnodes: NUMBER_OF_VNODES,
            backend: fash.BACKEND.IN_MEMORY
        });
        return cb(null, h1, h2);
    });
}

function _newRingFromDb(algo, callback) {
    var h1, h2;
    var location = '/tmp/' + uuid.v4();
    vasync.pipeline({funcs: [
        // create the ring using a cli in another process so that we can use it
        // in this process.
        function createRing(_, cb) {
            var eStr = FASH_CLI_PATH + ' create -v ' + NUMBER_OF_VNODES +
                ' -l ' + location + ' -p ' + PNODE_STRING + ' -b leveldb ' +
                '-a ' + algo;
            exec(eStr, function(err, stdout, stderr) {
                return cb(err);
            });
        },
        function createRingFromDb(_, cb) {
            h1 = fash.load({
                log: LOG,
                backend: fash.BACKEND.LEVEL_DB,
                location: location
            }, function(err) {
                if (err) {
                    return cb(err);
                }
                h2 = fash.create({
                    log: new Logger({
                        name: 'test-hash',
                        src: true,
                        level: 'fatal'
                    }),
                    algorithm: algo,
                    pnodes: PNODES,
                    vnodes: NUMBER_OF_VNODES,
                    backend: fash.BACKEND.IN_MEMORY
                });

                return cb();
            });
        }
    ], arg:{}}, function(err) {
        if (err) {
            err = new verror.VError(err);
        }
        return callback(err, h1, h2);
    });
}

function _newRingFromTopology(algo, cb) {
    var h1, h2;
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                h2 = hInMem;
                return cb(err);
            });
        },
        function serialize(_, cb) {
            _.hLevel.serialize(function(err, topology) {
                if (err) {
                    return cb(err);
                }
                _.topology = topology;
                return cb();
            });
        },
        function deserialize(_, cb) {
            h1 = fash.deserialize({
                log: LOG,
                topology: _.topology,
                backend: fash.BACKEND.LEVEL_DB,
                location: '/tmp/' + uuid.v4()
            }, function(err) {
                return cb(err);
            });
        }
    ], arg: {}}, function(err) {
        return cb(err, h1, h2);
    });
}

function _testAllAlgorithms(test) {
    ALGORITHM.forEach(function(algo) {
        exports[test.name + algo] = test.bind(null, algo);
    });
}

function _testAllConstructors(test) {
    ALGORITHM.forEach(function(algo) {
        exports[test.name + algo + 'new'] = test.bind(null, algo, _newRing);
        exports[test.name + algo + 'fromDb'] =
            test.bind(null, algo, _newRingFromDb);
        exports[test.name + algo + 'fromTopology'] =
            test.bind(null, algo, _newRingFromTopology);
    });
}
