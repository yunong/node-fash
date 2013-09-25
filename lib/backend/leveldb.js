/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */

var assert = require('assert-plus');
var bignum = require('bignum');
var bunyan = require('bunyan');
var common = require('../common');
var dtrace = require('dtrace-provider');
var fash = require('../index');
var crypto = require('crypto');
var levelup = require('levelup');
var once = require('once');
var util = require('util');
var sprintf = util.format;
var vasync = require('vasync');
var verror = require('verror');

/**
 * dtrace providers.
 */
var dtp = dtrace.createDTraceProvider("node-fash");
var d_levelPut = dtp.addProbe("levelPut", "char *", "char *");
dtp.enable();

/**
 * level db keys.
 */
var LKEY_VNODE_COUNT = 'VNODE_COUNT';
var LKEY_VNODE_DATA = 'VNODE_DATA';
var LKEY_VNODE_V = '/VNODE/%d';
var LKEY_PNODE = '/PNODE';
var LKEY_PNODE_P = '/PNODE/%s';
var LKEY_PNODE_P_V = '/PNODE/%s/%d';
var LKEY_ALGORITHM = 'ALGORITHM';
var LKEY_VERSION = 'VERSION';
var LKEY_COMPLETE = 'COMPLETE';

/**
 * leveldb values.
 */
var LVAL_NULL = 1;

/**
 * leveldb default config
 */
var LEVEL_CONFIG = {
    createIfMissing: true,
    errorIfExists: true,
    compression: false,
    cacheSize: 800 * 1024 * 1024,
    keyEncoding: 'utf8',
    valueEncoding: 'json'
};

/**
 * Creates an instance of ConsistentHash backed by leveldb.
 *
 * @constructor
 * @this {ConsistentHash}
 *
 * @param {Object} options The options object
 * @param {Object} options.log The optional Bunyan log object.
 * @param {Object} options.leveldbCfg The optional leveldb config object.
 * @param {Object} options.algorithm The hash algorithm object.
 * @param {String} options.algorithm.ALGORITHM The hash algorithm.
 * @param {String} options.algorithm.MAX The max output size of the algorithm.
 * @param {String} options.location The location on disk of the leveldb.
 * @param {Number} options.vnodes The number of virtual nodes in the ring. This
 *                 can't be changed once set.
 * @param {String[]} options.pnodes The optional array of physical nodes in the
 *                   ring, or the ring topology array.
 * @param {Object} topology The topology of a previous hash ring. Used to
 *                 restore an old hash ring.
 * @param {Object} topology.pnodeToVnodeMap The mapping of pnode to vnodes of
 *                 the serialized topology.
 * @param {Number} topology.vnodes The number of vnodes in the serialized
 *                 topology.
 */
function ConsistentHash(options, cb) {
    assert.object(options, 'options');
    assert.optionalObject(options.leveldbCfg, 'options.leveldbCfg');
    assert.string(options.location, 'options.location');
    assert.optionalBool(options.loadFromDb, 'options.loadFromDb');

    this.options_ = options;
    this.log = options.log;

    if (!this.log) {
        this.log = bunyan.createLogger({
            name: 'fash',
            level: (process.env.LOG_LEVEL || 'warn'),
            stream: process.stderr
        });
    }
    var self = this;
    var log = self.log;

    log.trace('new ConsistentHash with options', options);

    /**
     * the datapath leveldb
     */
    self.db_ = null;
    /**
     * The hash algorithm used determine the position of a key.
     */
    self.algorithm_ = null;

    /**
     * The leveldb configs.
     */
    self.leveldbCfg_ = options.leveldbCfg || LEVEL_CONFIG;
    /* regardless of what is set, always set keyEncoding to utf8 and
     * valueEncoding to json. Otherwise fash will not work.
     */
    self.leveldbCfg_.keyEncoding = LEVEL_CONFIG.keyEncoding;
    self.leveldbCfg_.valueEncoding = LEVEL_CONFIG.valueEncoding;

    /**
     * 1) create 'VNODE_COUNT' key which keeps track of the # of vnodes.
     * 2) create /VNODE/V keys which map vnodes to pnodes. The value is the
     * corresponding pnode.
     * 3) create /PNODE/<PNODE>/<VNODE> keys which map pnode to vnodes. The
     * value is the data of the vnode, defaults to 1 since leveldb requires a
     * value.
     * 4) create /PNODE/<PNODE> keys for all pnodes. The value is the set of
     * all vnodes that belong to this pnode. create /PNODE key which is an
     * array of all the pnodes.
     * 5) create algorithm key which contains the algorithm.
     * 6) create version key which contains the version.
     * 7) create complete key.
     */
    function createNewRing(callback) {
        log.info('instantiating new ring from scratch.');

        /*
         * The hash algorithm used determine the position of a key.
         */
        self.algorithm_ = options.algorithm;

        /*
         * The maximum output size of the hash algorithm. Used to determine the
         * hash interval between each vnode.
         */
        self.algorithmMax_ = bignum(options.algorithm.MAX, 16);

        /*
         * The number of virtual nodes to provision in the ring. Once set, this
         * can't be changed.
         */
        self.vnodeCount_ = options.vnodes || options.topology.vnodes || 100000;
        self.vnodesBignum_ = bignum(self.vnodeCount_, 10);
        self.algorithm_.VNODE_HASH_INTERVAL =
            self.algorithmMax_.div(self.vnodesBignum_);

        /*
         * The String array of physical nodes in the ring.
         */
        self.pnodes_ = options.pnodes ? options.pnodes.slice() : [];
        self.pnodes_.sort();

        var tasks = [
            function openDb(_, _cb) {
                _cb = once(_cb);
                levelup(options.location, self.leveldbCfg_, function(err, db) {
                    if (err) {
                        return _cb(new verror.VError(err));
                    }
                    if (!db) {
                        return _cb(
                            new verror.VError('unable to instantiate db!'));
                    }
                    self.db_ = db;
                    _.db = db;
                    return _cb();
                });
            },
            // step 1
            function putVnodeCount(_, _cb) {
                _cb = once(_cb);
                _.db.put(LKEY_VNODE_COUNT, self.vnodeCount_, function(err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            },
            /**
             * steps 2, 3
             * Allocate the vnodes to the pnodes by
             * vnode % total_pnode = assigned pnode.
             */
            function allocateVnodes(_, _cb) {
                _cb = once(_cb);
                _.pnodeToVnodeMap = {};
                for (var vnode = 0; vnode < self.vnodeCount_; vnode++) {
                    var pnode = self.pnodes_[vnode % self.pnodes_.length];
                    var hashspace = common.findHashspace({
                        vnode: vnode,
                        vnodeHashInterval: self.algorithm_.VNODE_HASH_INTERVAL,
                        log: log
                    });

                    log.debug({
                        hashspace: hashspace,
                        vnode: vnode,
                        pnode: pnode
                    }, 'ConsistentHash.new: assigning hashspace to vnode to ' +
                        'pnode');

                    /**
                     * assign the pnode->vnode and vnode->pnode maps
                     * set the data here to null since this is a new ring
                     */
                    _.db.put(sprintf(LKEY_VNODE_V, vnode),
                             pnode,
                             function(err)
                    {
                        if (err) {
                            err = new verror.VError(err);
                        }
                        return _cb(err);
                    });
                    /**
                     * we put the vnode in the path, to avoid having to put all
                     * vnodes under 1 key
                     */
                    var pnodePath = sprintf(LKEY_PNODE_P_V, pnode, vnode);
                    _.db.put(pnodePath, LVAL_NULL, function(err) {
                        if (err) {
                            err = new verror.VError(err);
                        }
                        return _cb(err);
                    });
                    // cache the pnopdeToVnode mapping for step 4
                    if (!_.pnodeToVnodeMap[pnode]) {
                        _.pnodeToVnodeMap[pnode] = [];
                    }
                    _.pnodeToVnodeMap[pnode].push(vnode);

                    log.debug({
                        vnode: vnode,
                        pnode: pnode
                    }, 'ConsistentHash.new: added vnode to pnode');
                }
                return _cb();
            },
            // step 4
            function writePnodeKeys(_, _cb) {
                _cb = once(_cb);
                var pnodeMap = {};
                for (var i = 0; i < self.pnodes_.length; i++) {
                    var pnode = self.pnodes_[i];
                    if (pnodeMap[pnode]) {
                        return _cb(new verror.VError('Unable to instantiate ' +
                                                     'duplicate pnodes'));
                    }
                    pnodeMap[pnode] = true;
                    log.debug({
                        pnode: '/PNODE/' + pnode,
                        vnodes: _.pnodeToVnodeMap[pnode]
                    }, 'writing vnode list for pnode');
                    _.db.put(sprintf(LKEY_PNODE_P, pnode),
                             _.pnodeToVnodeMap[pnode],
                            function(err)
                    {
                        if (err) {
                            err = new verror.VError(err);
                        }
                        return _cb(err);
                    });
                }
                 _.db.put(LKEY_PNODE, Object.keys(pnodeMap), function(err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                 });
            },
            function writeVnodeDataArray(_, _cb) {
                _cb = once(_cb);
                _.db.put(LKEY_VNODE_DATA, [], function(err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            },
            function writeMetadata(_, _cb) {
                // step 5
                // hacky clone the algorithm object.
                var algorithm = JSON.parse(JSON.stringify(self.algorithm_));
                algorithm.VNODE_HASH_INTERVAL =
                    self.algorithm_.VNODE_HASH_INTERVAL.toString(16);
                _.batch = _.db.batch().put(LKEY_ALGORITHM, algorithm);
                // step 6
                _.batch = _.batch.put(LKEY_VERSION, fash.VERSION);
                // step 7
                _.batch = _.batch.put(LKEY_COMPLETE, 1);
                return _cb();
            },
            function commit(_, _cb) {
                _.batch.write(function(err) {
                    if (err) {
                        err = new verror.VError(err);
                    }

                    return _cb(err);
                });
            }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
            if (err) {
                return callback(new verror.VError(err, 'unable to create ring'));
            } else {
                log.info('successfully instantiated new ring');
                return callback(null, self);
            }
        });
    }

    /**
     * 1) write vnodeCount.
     * 2) write /PNODE/PNODE, /PNODE/PNODE/VNODE, /VNODE/VNODE keys.
     * 3) write metadata.
     */
    function deserialize(callback) {
        var topology = options.topology;
        log.info('ConsistentHash.new.deserialize: deserializing an already ' +
                 'existing ring.');

        vasync.pipeline({funcs: [
            function openDb(_, _cb) {
                log.info('ConsistentHash.new.deserialize: opening db');
                //TODO: check to make sure there's nothing already at this
                //location
                levelup(options.location, self.leveldbCfg_, function(err, db) {
                    if (err) {
                        return _cb(new verror.VError(err));
                    }
                    if (!db) {
                        return _cb(
                            new verror.VError('unable to instantiate db!'));
                    }

                    self.db_ = db;
                    _.db = db;
                    return _cb();
                });
            },
            // step 1
            function putVnodeCount(_, _cb) {
                log.info('ConsistentHash.new.deserialize: put vnodeCount');
                _.db.put(LKEY_VNODE_COUNT, topology.vnodes, function(err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            },
            // step 2
            function allocateVnodes(_, _cb) {
                _cb = once(_cb);
                log.info('ConsistentHash.new.deserialize: allocate vnodes');
                var vnodeData = [];
                var pvMap = topology.pnodeToVnodeMap;

                // /PNODE
                var pnodes = Object.keys(pvMap);
                _.db.put(LKEY_PNODE, pnodes, function(err) {
                    if (err) {
                        err = new verror.VError(err);
                        return _cb(err);
                    }
                });

                // /VNODE/V, /PNODE/P, /P/P/V
                var pcount = pnodes.length;
                pnodes.forEach(function(pnode) {
                    var vnodes = Object.keys(pvMap[pnode]);
                    var vcount = vnodes.length;

                    // write /P/P and /V/V. and /P/P/V
                    vnodes.forEach(function(vnode, index) {

                        // json serializes vnode into a string, we need to
                        // parse it back into an integer before we store it
                        vnodes[index] = parseInt(vnode, 10);

                        // write /V/V
                        dtp.fire("levelPut", function(p) {
                            return[sprintf(LKEY_VNODE_V, vnode), pnode];
                        });
                        _.db.put(sprintf(LKEY_VNODE_V, vnode), pnode,
                               function(err)
                        {
                            if (err) {
                                err = new verror.VError(err);
                                return _cb(err);
                            }
                        });
                        // write /P/P/V
                        dtp.fire("levelPut", function(p) {
                            return[sprintf(LKEY_PNODE_P_V, pnode, vnode),
                                pvMap[pnode][vnode]];
                        });
                        _.db.put(
                            sprintf(LKEY_PNODE_P_V, pnode, vnode),
                            pvMap[pnode][vnode],
                            function(err) {
                                if (err) {
                                    err = new verror.VError(err);
                                    return _cb(err);
                                }
                            }
                        );

                        // put the vnode in the VNODE_DATA array if it contains
                        // data
                        if (pvMap[pnode][vnode] !== LVAL_NULL) {
                            vnodeData.push(vnode);
                        }

                        // write /P/P once all the vnodes have been parsed back
                        // into ints.
                        vcount--;
                        if (vcount === 0) {
                            dtp.fire("levelPut", function(p) {
                                return[sprintf(LKEY_PNODE_P, pnode), vnodes];
                            });

                            _.db.put(
                                sprintf(LKEY_PNODE_P, pnode),
                                vnodes,
                                function(err) {
                                    if (err) {
                                        err = new verror.VError(err);
                                        return _cb(err);
                                    }
                                }
                            );
                        }

                        // write the VNDOE_DATA array.
                        if (vcount === 0 && --pcount === 0) {
                            dtp.fire("levelPut", function(p) {
                                return[LKEY_VNODE_DATA, vnodeData];
                            });
                            _.db.put(LKEY_VNODE_DATA, vnodeData, function(err) {
                                if (err) {
                                    err = new verror.VError(err);
                                }
                                return _cb(err);
                            });
                        }
                    });
                });
            },
            // step 3
            function writeMetadata(_, _cb) {
                log.info('ConsistentHash.new.deserialize: write metadata');
                // hacky clone
                self.algorithm_ = topology.algorithm;
                var algorithm = JSON.parse(JSON.stringify(self.algorithm_));
                var batch = self.db_.batch();
                algorithm.VNODE_HASH_INTERVAL =
                    self.algorithm_.VNODE_HASH_INTERVAL.toString(16);

                batch.put(LKEY_ALGORITHM, algorithm).
                    put(LKEY_VERSION, fash.VERSION).
                    put(LKEY_COMPLETE, 1);
                batch.write(function(err) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            }
        ], arg: {}}, function(err) {
            if (err) {
                return callback(new verror.VError(err, 'unable to ' +
                                                  'deserialize ring'));
            } else {
                log.info('successfully deserialized ring');
                return callback(null, self);
            }
        });
    }

    function loadFromDb(callback) {
        // don't create if loading from db
        self.leveldbCfg_.createIfMissing = false;
        self.leveldbCfg_.errorIfExists = false;
        vasync.pipeline({funcs: [
            function openDb(_, _cb) {
                levelup(options.location, self.leveldbCfg_, function(err, db) {
                    if (err) {
                        return _cb(new verror.VError(err));
                    }
                    if (!db) {
                        return _cb(
                            new verror.VError('unable to instantiate db!'));
                    }
                    self.db_ = db;
                    return _cb();
                });
            },
            function checkComplete(_, _cb) {
                self.db_.get(LKEY_COMPLETE, function(err, value) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    return _cb(err);
                });
            },
            function checkVersion(_, _cb) {
                _cb = once(_cb);
                self.db_.get(LKEY_VERSION, function(err, version) {
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
            function checkVnodes(_, _cb) {
                self.db_.get(LKEY_VNODE_COUNT, function(err, vnodeCount) {
                    if (err) {
                        err = new verror.VError(err);
                    }

                    _.vnodeCount = vnodeCount;
                    return _cb(err);
                });
            },
            function checkSlashVnodeSlashN(_, _cb) {
                // spot check /vnode/largestVnode
                self.db_.get(sprintf(LKEY_VNODE_V, _.vnodeCount - 1),
                             function(err, pnode) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    assert.string(pnode, 'pnode');
                    return _cb(err);
                });
            },
            function getAlgorithm(_, _cb){
                self.db_.get(LKEY_ALGORITHM, function(err, algorithm) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    algorithm.VNODE_HASH_INTERVAL =
                        bignum(algorithm.VNODE_HASH_INTERVAL, 16);
                    self.algorithm_ = algorithm;
                    return _cb(err);
                });
            }
        ], arg: {}}, function(err) {
            if (err) {
                return callback(new verror.VError(err, 'unable to ' +
                                                  'load ring from db'));
            } else {
                log.info('successfully loaded ring from db');
                return callback(null, self);
            }
        });
    }

    if (options.topology) {
        deserialize(cb);
    } else if (options.loadFromDb) {
        loadFromDb(cb);
    } else {
        createNewRing(cb);
    }
}

/**
 * @exports ConsistentHash as Consistenthash
 */
module.exports = ConsistentHash;

/**
 * Gets the pnode and vnode that a key belongs to on the ring.
 *
 * @param {String} key The key.
 * @param {function} cb The callback f(err, {pnode, vnode, data}).
 */
ConsistentHash.prototype.getNode = function getNode(key, callback) {
    var self = this;
    var value = crypto.createHash(this.algorithm_.NAME).update(key).digest('hex');
    // find the node that corresponds to this hash
    var vnode = common.findVnode({
        hash: value, vnodeHashInterval: self.algorithm_.VNODE_HASH_INTERVAL
    });
    self.db_.get(sprintf(LKEY_VNODE_V, vnode), function(err, pnode) {
        if (err) {
            return callback(err);
        }
        self.db_.get(sprintf(LKEY_PNODE_P_V, pnode, vnode),
                     function(_err, data)
        {
            if (_err) {
                return callback(_err);
            }
            return callback(null, {pnode: pnode, vnode: vnode, data: data});
        });

        return (undefined);
    });
};


/**
 * Adds a data element to a vnode. If data already existed for the vnode, this
 * will over-write it. This is useful if you want to add stateful changes to a
 * set of particular vnodes -- for example, setting some vnodes to read only.
 *
 * @param {Number} vnode The vnode to add data to.
 * @param {Object} data The data to add to the vnode.
 * @param {function} cb The callback f(err).
 */
ConsistentHash.prototype.addData = function addData(vnode, data, cb) {
    var self = this;
    var log = self.log;
    var db = self.db_;

    log.info({
        vnode: vnode,
        data: data
    }, 'Consistenthash.addData: entering');

    assert.number(vnode, 'vnode');
    assert.func(cb, 'callback');
    assert.optionalString(data, 'data');
    if (!data) {
        data = LVAL_NULL;
    }

    vasync.pipeline({funcs: [
        function getPnode(_, _cb) {
            db.get(sprintf(LKEY_VNODE_V, vnode), function(err, pnode) {
                if (err) {
                    return _cb(new verror.VError(err, 'unable to add data'));
                }
                log.info({
                    vnode: vnode,
                    pnode: pnode
                }, 'Consistenthash.addData: vnode maps to');

                _.pnode = pnode;
                return _cb();
            });
        },
        function getVnodeDataArray(_, _cb) {
            db.get(LKEY_VNODE_DATA, function(err, vnodeData) {
                if (err) {
                    return _cb(new verror.VError(err, 'unable to add data'));
                }
                log.info({
                    vnodeData: vnodeData
                }, 'ConsistentHash.addData: vnodeDataArray');
                var idx = vnodeData.indexOf(vnode);
                if (data === LVAL_NULL) {
                    // if the vnode exists, then remove it
                    if (idx !== -1) {
                        vnodeData.splice(idx, 1);
                        _.vnodeData = vnodeData;
                    }
                } else { // data is not null and the vnode isn't in the array
                    if (idx === -1) {
                        vnodeData.push(vnode);
                        _.vnodeData = vnodeData;
                    }
                }

                return _cb();
            });
        },
        function setData(_, _cb) {
            _.batch = db.batch().put(sprintf(LKEY_PNODE_P_V, _.pnode, vnode),
                                     data);

            if (_.vnodeData) {
                _.batch.put(LKEY_VNODE_DATA, _.vnodeData);
            }

            return _cb();
        },
        function commit(_, _cb) {
            _.batch.write(function(err) {
                if (err) {
                    err = new verror.VError(err, 'addData: unable to commit');
                }

                return _cb(err);
            });
        }
    ], arg:{}}, function(err) {
        return cb(err);
    });
};

/**
 * Get the list of vnodes with data in them.
 *
 * @param {function} cb The callback f(err, {})
 */
ConsistentHash.prototype.getDataVnodes = function getDataVnodes(cb) {
    var self = this;
    var log = self.log;
    var db = self.db_;

    log.info('ConsistentHash.getDataVnodes: entering');
    assert.func(cb, 'callback');

    db.get(LKEY_VNODE_DATA, function(err, vnodeArray) {
        if (err) {
            err = new verror.VError(err);
        }

        return cb(err, vnodeArray);
    });
};

/**
 * Remaps a pnode on the hash ring. The node can be an existing pnode, or a new
 * one.
 *
 * @param {String} node The name of the node.
 * @param {Number} The vnode to add to this pnode. Implicitly removes the
 *                   vnode from its previous pnode owner.
 * @param {function} cb The callback f(err).
 */
ConsistentHash.prototype.remapVnode = function remapVnode(newPnode, vnode, cb) {
    var self = this;
    var log = self.log;
    var db = self.db_;
    log.info({
        newNode: newPnode,
        vnode: vnode
    }, 'ConsistentHash.remapVnode: entering');
    assert.string(newPnode, 'newPnode');
    assert.number(vnode, 'vnode');
    assert.func(cb, 'callback');

    /**
     * assert the vnodes, ensuring that:
     * 1) vnode actually exists.
     * 2) vnode doesn't already belong to the newPnode.
     * 3) get the old pnode the vnode belongs to.
     * 4) get the data of the vnode.
     * 5) delete the old mappings, /pnode/oldp/n, remove vnode from the array
     * in /pnode/oldp.
     * 6) add the new mappings, /pnode/newp/n, add vnode to the array in
     * /pnode/newp, and map /vnode/n to the new pnode.
     */
    vasync.pipeline({funcs: [
        function assertVnodeExists(_, _cb) {
            if ((vnode > self.vnodeCount_) || (vnode < 0)) {
                return _cb(new verror.VError('vnode ' + vnode +
                                            ' does not exist in the ring'));
            } else {
                return _cb();
            }
        },
        function initBatch(_, _cb) {
            _.batch = db.batch();
            return _cb();
        },
        function checkAndCreateNewPnode(_, _cb) {
            db.get(sprintf(LKEY_PNODE_P, newPnode), function(err) {
                if (err && err.name && err.name === 'NotFoundError') {
                    _.batch = _.batch.put(sprintf(LKEY_PNODE_P, newPnode), []);
                    _.isNew = true;
                    return _cb();
                } else {
                    if (err) {
                        return _cb(new verror.VError(err));
                    } else {
                        return _cb();
                    }
                }
            });
        },
        function getOldVnodeMapping(_, _cb) {
            // get the previous vnode to pnode mapping
            db.get(sprintf(LKEY_VNODE_V, vnode), function(err, pnode) {
                if (err) {
                    return _cb(new verror.VError(err));
                }
                _.oldPnode = pnode;
                // check that the vnode doesn't already belong to the newPnode.
                if (pnode === newPnode) {
                    return _cb(new verror.VError('vnode ' + vnode +
                                                ' already belongs to pnode'));
                } else {
                    return _cb();
                }
            });
        },
        function getVnodeData(_, _cb) {
            db.get(sprintf(LKEY_PNODE_P_V, _.oldPnode, vnode), function(err, d)
            {
                if (err) {
                    return _cb(err);
                } else {
                    _.data = d;
                    return _cb();
                }
            });
        },
        function delOldMapping(_, _cb) {
            _.batch = _.batch.del(sprintf(LKEY_PNODE_P_V, _.oldPnode, vnode));
            db.get(sprintf(LKEY_PNODE_P, _.oldPnode), function(err, oldVnodes) {
                if (err) {
                    return _cb(new verror.VError(err,
                                                'couldn\'t get path /pnode/' +
                                                 _.oldPnode));
                }
                var idx = oldVnodes.indexOf(vnode);
                if (idx === -1) {
                    return _cb(new verror.VError('vnode: ' + vnode +
                                                ' does not ' +
                                                'exist in old pnode: ' +
                                                _.oldPnode));
                }
                oldVnodes.splice(idx, 1);
                _.batch = _.batch
                .put(sprintf(LKEY_PNODE_P, _.oldPnode), oldVnodes)
                .del(sprintf(LKEY_PNODE_P_V, _.oldPnode, vnode));
                return _cb();
            });
        },
        function addNewMapping(_, _cb) {
            db.get(sprintf(LKEY_PNODE_P, newPnode), function(err, vnodes) {
                /*
                 * ignore NotFoundErrors if the pnode is new, since it hasn't
                 * been created yet
                 */
                if (err &&
                    !(_.isNew && err && err.name &&
                      err.name === 'NotFoundError'))
                {
                    return _cb(new verror.VError(err));
                }
                if (_.isNew && !vnodes) {
                    vnodes = [];
                }
                vnodes.push(vnode);
                _.batch = _.batch.put(sprintf(LKEY_PNODE_P, newPnode), vnodes)
                .put(sprintf(LKEY_PNODE_P_V, newPnode, vnode), _.data)
                .put(sprintf(LKEY_VNODE_V, vnode), newPnode);
                return _cb();
            });
        },
        function addPnodeToPnodeArray(_, _cb) {
            db.get(LKEY_PNODE, function(err, pnodes) {
                if (err) {
                    return _cb(new verror.VError(err));
                }

                // add the new pnode to the pnode array if it doesn't exist.
                if (pnodes.indexOf(newPnode) === -1) {
                    pnodes.push(newPnode);
                    _.batch = _.batch.put(LKEY_PNODE, pnodes);
                }
                return _cb();
            });
        },
        function commit(_, _cb) {
            _.batch.write(function(err) {
                if (err) {
                    return _cb(new verror.VError(err));
                } else {
                    return _cb();
                }
            });
        }
    ], arg: {}}, function(err) {
        log.info({err: err}, 'ConsistentHash.remapVnode: exiting');
        return cb(err);
    });
};

/**
 * Get the array of vnodes that belong to a particular pnode
 *
 * @param {String} pnode The pnode.
 * @param {function} cb The callback f(err, Array).
 */
ConsistentHash.prototype.getVnodes = function getVnodes(pnode, cb) {
    var self = this;
    var log = self.log;
    var db = self.db_;

    log.info({pnode: pnode}, 'ConsistentHash.getVnodes: entering');
    assert.string(pnode, 'pnode');
    assert.func(cb, 'callback');

    // check that the pnode exists
    db.get(sprintf(LKEY_PNODE_P, pnode), function(err, vnodes) {
        if (err) {
            return cb(new verror.VError(err, 'pnode is not in ring'));
        }
        log.info({
            pnode: pnode,
            vnodes: vnodes
        }, 'ConsistentHash.getVnodes: exiting');
        return cb(null, vnodes);
    });
};

/**
 * Get the array of pnodes that's in this hash.
 *
 * @param {function} cb The callback f(err, map).
 */
ConsistentHash.prototype.getPnodes = function getPnodes(cb) {
    var self = this;
    var log = self.log;
    var db = self.db_;

    log.info('ConsistentHash.getPnodes: entering');

    assert.func(cb, 'callback');
    db.get(LKEY_PNODE, function(err, pnodes) {
        if (err) {
            err = new verror.VError(err, 'unable to get Pnodes');
        }

        return cb(err, pnodes);
    });
};

/**
 * Removes a pnode from the hash ring.  Note the pnode must not map to any
 * vnodes.  Remove the vnodes first by re-assigning them to other pnodes before
 * invoking this function.
 *
 * @param {String} pnode The pnode to remove.
 * @param {function} cb The callback f(err).
 */
ConsistentHash.prototype.removePnode = function removePnode(pnode, cb) {
    var self = this;
    var log = self.log;
    var db = self.db_;

    log.info({
        pnode: pnode
    }, 'ConsistentHash.removePnode: entering');

    assert.string(pnode, 'pnode');
    assert.func(cb, 'callback');
    // check that the pnode exists
    vasync.pipeline({funcs: [
        function checkPnodeExists(_, _cb){
            db.get(sprintf(LKEY_PNODE_P, pnode), function(err, v) {
                if (err) {
                    return _cb(new verror.VError(err, 'pnode does not exist'));
                }
                _.vnodes = v;
                return _cb();
            });
        },
        function checkPnodeHasVnodes(_, _cb) {
            if (_.vnodes && _.vnodes.length > 0) {
                var errMsg = 'pnode still maps to vnodes, ' +
                             're-assign vnodes first';
                return _cb(new verror.VError(errMsg));
            }
            return _cb();
        },
        function _removePnode(_, _cb) {
            // remove /pnode/%s
            var batch = db.batch().del(sprintf(LKEY_PNODE_P, pnode));
            // get the pnode array
            db.get(LKEY_PNODE, function(err, pnodes) {
                if (err) {
                    return _cb(new verror.VError(err));
                }

                // remove the pnode to the pnode array if it doesn't exist.
                var pnodeIndex = pnodes.indexOf(pnode);
                if (pnodeIndex === -1) {
                    return _cb(new verror.VError('pnode does not exist'));
                }
                pnodes.splice(pnodeIndex, 1);
                batch.put(LKEY_PNODE, pnodes).write(function(_err) {
                    if (_err) {
                        _err = new verror.VError(_err);
                    }
                    log.info({
                        err: _err,
                        pnode: pnode
                    }, 'ConsistentHash.removePnode: exiting');
                    return _cb(_err);
                });
                return (undefined);
            });
        }
    ], arg: {}}, function(err) {
        return cb(err);
    });
};

/**
 * Serialize the current state of the ring in serialized to a JSON string.
 *
 * @param {Function} callback The callback of the form f(err, Object).
 * @return {Object} ring The updated ring topology.
 * @return {Object} ring.pnodeToVnodeMap The map of {pnode->{vnode1,... vnoden}.
 * @return {String} ring.vnode The number of vnodes in the ring.
 * @return {String} ring.algorithm The algorithm used in the ring.
 * @return {String} ring.version The version of the ring.
 */
ConsistentHash.prototype.serialize = function serialize(callback) {
    var self = this;
    var log = self.log;
    var db = self.db_;
    log.info('ConsistentHash.serialize: entering');
    assert.func(callback, 'callback');

    var serializedHash = {
        vnodes: null,
        pnodeToVnodeMap: {},
        algorithm: null,
        version: null
    };

    var tasks = [
        function getNumberOfVnodes(_, cb) {
            db.get(LKEY_VNODE_COUNT, function(err, vnodes) {
                serializedHash.vnodes = vnodes;
                return cb(err);
            });
        },
        function getVnodeToPnodeMaps(_, cb) {
            cb = once(cb);
            var count = serializedHash.vnodes;
            for (var vnode = 0; vnode < serializedHash.vnodes; vnode++) {
                db.get(sprintf(LKEY_VNODE_V, vnode),
                       (function(v, err, pnode)
                {
                    if (err) {
                        return cb(new verror.VError(err));
                    }
                    serializedHash.pnodeToVnodeMap[pnode] = {};
                    db.get(sprintf(LKEY_PNODE_P_V, pnode, v),
                           function(_err, data)
                    {
                        if (_err) {
                            return cb(new verror.VError(_err));
                        }
                        serializedHash.pnodeToVnodeMap[pnode][v] = data;
                        if (--count === 0) {
                            return cb();
                        }
                        return (undefined);
                    });
                    return (undefined);
                }).bind(this, vnode));
            }
        },
        function getAlgorithm(_, cb){
            db.get(LKEY_ALGORITHM, function(err, algorithm) {
                if (err) {
                    err = new verror.VError(err);
                }
                serializedHash.algorithm = algorithm;
                return cb(err);
            });
        },
        function getVersion(_, cb){
            db.get(LKEY_VERSION, function(err, version) {
                if (err) {
                    err = new verror.VError(err);
                }
                serializedHash.version = version;
                return cb(err);
            });
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
        if (err) {
            return callback(new verror.VError(err, 'unable to serialize ring'));
        } else {
            log.trace({
                serializedRing: serializedHash
            }, 'ConsistentHash.serialize: fully serialized ring');
            log.info('ConsistentHash.serialize: exiting');
            return callback(null, JSON.stringify(serializedHash));
        }
    });
};

/**
 * used for unit tests only.
 */
module.exports.LKEY_ALGORITHM = LKEY_ALGORITHM;
module.exports.LKEY_COMPLETE = LKEY_COMPLETE;
module.exports.LKEY_PNODE_P = LKEY_PNODE_P;
module.exports.LKEY_PNODE_P_V = LKEY_PNODE_P_V;
module.exports.LKEY_VERSION = LKEY_VERSION;
module.exports.LKEY_VNODE_COUNT = LKEY_VNODE_COUNT;
module.exports.LKEY_VNODE_V = LKEY_VNODE_V;

