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
        function debugPrintVnodes(_, cb) {
            if (LOG.level() <= 30) {
                _.hLevel.getVnodes(PNODES[1], function(err, vnodes) {
                    LOG.debug({
                        levelVnodes: vnodes,
                        inmemVnodes: _.hInMem.getVnodes(PNODES[1]),
                        inmemOldVnodes: _.hInMem.getVnodes(PNODES[0])
                    });
                    return cb(err);
                });
            } else {
                return cb();
            }
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
            LOG.debug({err: err, key: k, node1: node1, node2: node2, count: count}, 'returned from getNode');
            if (err) {
                t.fail(err);
                return cb();
            }
            t.strictEqual(node1.pnode, node2.pnode, 'hashed node ' + util.inspect(node1) +
                          ' does not match test in-mem hash' + util.inspect(node2));
            t.strictEqual(node1.vnode, node2.vnode, 'hashed node ' + util.inspect(node1) +
                          ' does not match test in-mem hash' + util.inspect(node2));
            t.strictEqual(node1.data, node2.data, 'hashed node ' + util.inspect(node1) +
                          ' does not match test in-mem hash' + util.inspect(node2));
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

function _testAllAlgorithms(test) {
    ALGORITHM.forEach(function(algo) {
        exports[test.name + algo] = test.bind(null, algo);
    });
}
