var bignum = require('bignum');
var crypto = require('crypto');
var common = require('../lib/common');
var fash = require('../lib');
var Logger = require('bunyan');
var util = require('util');
var uuid = require('node-uuid');
var vasync = require('vasync');

var LOG = new Logger({
    name: 'consistent-hash-test',
    src: true,
    level: process.env.LOG_LEVEL || 'warn'
});
var NUMBER_OF_KEYS = parseInt(process.env.NUMBER_OF_KEYS || 1000, 10);
var NUMBER_OF_VNODES = parseInt(process.env.NUMBER_OF_VNODES || 100);
var NUMBER_OF_PNODES = parseInt(process.env.NUMBER_OF_PNODES || 10);
var PNODES = new Array(NUMBER_OF_PNODES);
var ALGORITHM = ['sha256', 'sha1', 'md5'];
var LEVELDB_LOCATION = process.env.LEVELDB_LOCATION || './test/data/leveldb-';

exports.beforeTest = function(t) {
    for (var i = 0; i < NUMBER_OF_PNODES; i++) {
        PNODES[i] = 'pnode' + i;
    }

    PNODES.sort();
    t.done();
};

_testAllAlgorithms(function newRing(algo, t) {
    _newRing(algo, function(err, hLevel, hInMem) {
        if (err) {
            t.fail(err);
            t.done();
        }
        _verifyRing(hLevel, hInMem, t, algo, function() {
            t.done();
        });
    });
});

_testAllAlgorithms(function remapOnePnodeToAnother(algo, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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
                t.strictEqual(JSON.stringify(vnodes.sort()),
                              JSON.stringify(inMemVnodes.sort()),
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

_testAllAlgorithms(function remapSomeVnodeToAnother(algo, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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
                t.strictEqual(JSON.stringify(vnodes.sort()),
                              JSON.stringify(inMemVnodes.sort()),
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

_testAllAlgorithms(function removePnode(algo, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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
                t.strictEqual(JSON.stringify(vnodes.sort()),
                              JSON.stringify(inMemVnodes.sort()),
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

_testAllAlgorithms(function addNewPnode(algo, t) {
    var newPnode = 'yunong';
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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
            _.hLevel.getVnodes(newPnode, function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(newPnode);
                t.strictEqual(JSON.stringify(vnodes.sort()),
                              JSON.stringify(inMemVnodes.sort()),
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

_testAllAlgorithms(function addNewPnodeRemapOnlySubsetOfOldPnode(algo, t) {
    var newPnode = 'yunong';
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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
            _.hLevel.getVnodes(newPnode, function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(newPnode);
                t.strictEqual(JSON.stringify(vnodes.sort()),
                              JSON.stringify(inMemVnodes.sort()),
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

_testAllAlgorithms(function addData(algo, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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

_testAllAlgorithms(function addDataRemapVnodeToDifferentPnode(algo, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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
            _.hLevel.getVnodes(_.newPnode, function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(_.newPnode);
                t.strictEqual(JSON.stringify(vnodes.sort()),
                              JSON.stringify(inMemVnodes.sort()),
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

_testAllAlgorithms(function addDataRemapVnodeToNewPnode(algo, t) {
    var newPnode = 'yunong';
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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
            _.hLevel.getVnodes(newPnode, function(err, vnodes) {
                if (err) {
                    return cb(err);
                }
                var inMemVnodes = _.hInMem.getVnodes(newPnode);
                t.strictEqual(JSON.stringify(vnodes.sort()),
                              JSON.stringify(inMemVnodes.sort()),
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

_testAllAlgorithms(function serialize(algo, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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
             //sometimes this might fail since JSON.stringify doesn't guarantee
             //order, meh.
            t.strictEqual(_.topology, inMemTopology, 'topology should match ' +
                          'in mem test version');
            return cb();
        }
    ], arg: {}}, function(err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});

_testAllAlgorithms(function deserialize(algo, t) {
    vasync.pipeline({funcs: [
        function newRing(_, cb) {
            _newRing(algo, function(err, hLevel, hInMem) {
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
        function deserialize(_, cb) {
            _newRingFromTopology(_.topology, function(err, hLevel, hInMem) {
                _.hLevel = hLevel;
                _.hInMem = hInMem;
                return cb(err);
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

_testAllAlgorithms(function loadFromDb(algo, t) {
    vasync.pipeline({funcs: [
        function newRingFromDb(_, cb) {
            _newRingFromDb(algo, function(err, h1, h2) {
                _.hLevel = h1;
                _.hInMem = h2;
                return cb(err);
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

// private helpers
function _verifyRing(h1, h2, t, algo, cb) {
    // XXX check validity of keys in leveldb.

    // compare hashing of in_memory hash to lebeldb hash
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
                t.fail(err);
                return cb();
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

function _newRingFromDb(algo, cb) {
    var h1 = fash.load({
        log: LOG,
        backend: fash.BACKEND.LEVEL_DB,
        location: LEVELDB_LOCATION + algo,
    }, function(err) {
        if (err) {
            return cb(err);
        }
        // XXX need a sample leveldb for each algorithm.
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

function _newRingFromTopology(topology, cb) {
    var h1 = fash.deserialize({
        log: LOG,
        topology: topology,
        backend: fash.BACKEND.LEVEL_DB,
        location: '/tmp/' + uuid.v4()
    }, function(err) {
        if (err) {
            return cb(err);
        }
        var h2 = fash.deserialize({
            log: new Logger({
                name: 'test-hash',
                src: true,
                level: 'fatal'
            }),
            topology: topology,
            backend: fash.BACKEND.IN_MEMORY
        });
        return cb(null, h1, h2);
    });
}

function _testAllAlgorithms(test) {
    ALGORITHM.forEach(function(algo) {
        exports[test.name + algo] = test.bind(null, algo);
    });
}
