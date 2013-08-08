/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */

var assert = require('assert-plus');
var bignum = require('bignum');
var bunyan = require('bunyan');
var common = require('../common');
var fash = require('../index');
var crypto = require('crypto');
var levelup = require('levelup');
var once = require('once');
var util = require('util');
var sprintf = util.format;
var vasync = require('vasync');
var verror = require('verror');


/**
 * Creates an instance of ConsistentHash.
 *
 * @constructor
 * @this {ConsistentHash}
 *
 * @param {Object} options The options object
 * @param {Object} options.log The optional Bunyan log object.
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
     * 1) create 'vnodeCount' key which keeps track of the # of vnodes.
     * 2) create /vnode/n keys which map vnodes to pnodes. The values is the
     * pnode.
     * 3) create /pnode/<pnode>/<vnode> keys which map pnode to vnodes. The
     * value is the data of the vnode, defaults to 1 since leveldb requires a
     * value.
     * 4) create /pnode/<pnode> keys for all pnodes. The value is the set of
     * all vnodes that belong to this pnode.
     * 5) create algorithm key which contains the algorithm.
     * 6) create version key which contains the version.
     * 7) create complete key.
     */
    function createNewRing(callback) {
        log.info('instantiating new ring from scratch.');

        /**
         * The hash algorithm used determine the position of a key.
         */
        self.algorithm_ = options.algorithm;

        /**
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

        /**
         * The String array of physical nodes in the ring.
         */
        self.pnodes_ = options.pnodes ? options.pnodes.slice() : [];
        self.pnodes_.sort();

        var tasks = [
            function openDb(_, _cb) {
                levelup(options.location, {
                    createIfMissing: true,
                    compression: false,
                    cacheSize: 800 * 1024 * 1024,
                    keyEncoding: 'utf8',
                    valueEncoding: 'json'
                }, function(err, db) {
                    if (err) {
                        return _cb(err);
                    }
                    self.db_ = db;
                    _.batch = db.batch();
                    return _cb();
                });
            },
            // step 1
            function putVnodeCount(_, _cb) {
                _.batch = _.batch.put('vnodeCount', self.vnodeCount_);
                return _cb();
            },
            /**
             * steps 2, 3
             * Allocate the vnodes to the pnodes by
             * vnode % total_pnode = assigned pnode.
             */
            function allocateVnodes(_, _cb) {
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
                    _.batch = _.batch.put(sprintf('/vnode/%d', vnode), pnode);
                    /**
                     * we put the vnode in the path, to avoid having to put all
                     * vnodes under 1 key
                     */
                    var pnodePath = sprintf('/pnode/%s/%d', pnode, vnode);
                    _.batch = _.batch.put(pnodePath, 1);
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
                var pnodeMap = {};
                for (var i = 0; i < self.pnodes_.length; i++) {
                    var pnode = self.pnodes_[i];
                    if (pnodeMap[pnode]) {
                        throw new verror.VError('Unable to instantiate ring, ' +
                                                'duplicate pnodes in input');
                    }
                    pnodeMap[pnode] = true;
                    log.debug({
                        pnode: '/pnode/' + pnode,
                        vnodes: _.pnodeToVnodeMap[pnode]
                    }, 'writing vnode list for pnode');
                    _.batch = _.batch.put(sprintf('/pnode/%s', pnode),
                                         _.pnodeToVnodeMap[pnode]);
                }
                return _cb();
            },
            function writeMetadata(_, _cb) {
                // step 5
                // hacky clone
                var algorithm = JSON.parse(JSON.stringify(self.algorithm_));
                algorithm.VNODE_HASH_INTERVAL =
                    self.algorithm_.VNODE_HASH_INTERVAL.toString(16);
                _.batch = _.batch.put('algorithm', algorithm);
                // step 6
                _.batch = _.batch.put('version', fash.VERSION);
                // step 7
                _.batch = _.batch.put('complete', 1);
                return _cb();
            },
            function commit(_, _cb) {
                _.batch.write(_cb);
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
     * 2) write /pnode/pnode, /pnode/pnode/vnode, /vnode/vnode keys.
     * 3) write metadata.
     */
    function deserialize(callback) {
        var topology = options.topology;
        log.info('ConsistentHash.new.deserialize: deserializing an already ' +
                 'existing ring.');

        vasync.pipeline({funcs: [
            function openDb(_, _cb) {
                log.info('ConsistentHash.new.deserialize: opening db');
                levelup(options.location, {
                    createIfMissing: true,
                    compression: false,
                    cacheSize: 800 * 1024 * 1024,
                    keyEncoding: 'utf8',
                    valueEncoding: 'json'
                }, function(err, db) {
                    if (err) {
                        return _cb(err);
                    }
                    self.db_ = db;
                    _.batch = db.batch();
                    return _cb();
                });
            },
            // step 1
            function putVnodeCount(_, _cb) {
                log.info('ConsistentHash.new.deserialize: put vnodeCount');
                _.batch = _.batch.put('vnodeCount', topology.vnodes);
                return _cb();
            },
            // step 2
            function allocateVnodes(_, _cb) {
                log.info('ConsistentHash.new.deserialize: allocate vnodes');
                var pvMap = topology.pnodeToVnodeMap;
                // /vnode/n, /pnode/pnode, /p/p/v
                var pnodes = Object.keys(pvMap);
                var pcount = pnodes.length;
                pnodes.forEach(function(pnode) {
                    var vnodes = Object.keys(pvMap[pnode]);
                    var vcount = vnodes.length;
                    // write /p/p and /v/v. and /p/p/v
                    vnodes.forEach(function(vnode, index) {
                        // json serializes vnode into a string, we need to
                        // parse it back into an integer before we store it
                        vnodes[index] = parseInt(vnode, 10);
                        // write /v/v
                        _.batch = _.batch.put(sprintf('/vnode/%d', vnode),
                                              pnode);
                        // write /p/p/v
                        _.batch = _.batch.put(
                            sprintf('/pnode/%s/%d', pnode, vnode),
                            pvMap[pnode][vnode]
                        );
                        // write /p/p once all the vnodes have been parsed back
                        // into ints.
                        vcount--;
                        if (vcount === 0) {
                            _.batch = _.batch.put(sprintf('/pnode/%s', pnode), vnodes);
                        }
                        if (vcount === 0 && --pcount === 0) {
                            _cb();
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
                algorithm.VNODE_HASH_INTERVAL =
                    self.algorithm_.VNODE_HASH_INTERVAL.toString(16);
                _.batch = _.batch.put('algorithm', algorithm);

                _.batch = _.batch.put('version', fash.VERSION);
                _.batch = _.batch.put('complete', 1);
                return _cb();
            },
            function commit(_, _cb) {
                _.batch.write(_cb);
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
        vasync.pipeline({funcs:[
            function openDb(_, _cb) {
                levelup(options.location, {
                    createIfMissing: false,
                    compression: false,
                    cacheSize: 800 * 1024 * 1024,
                    keyEncoding: 'utf8',
                    valueEncoding: 'json'
                }, function(err, db) {
                    self.db_ = db;
                    return _cb(err);
                });
            },
            // TODO: add checks
            function getAlgorithm(_, _cb){
                self.db_.get('algorithm', function(err, algorithm) {
                    if (err) {
                        err = new verror.VError(err);
                    }
                    algorithm.VNODE_HASH_INTERVAL =
                        bignum(algorithm.VNODE_HASH_INTERVAL, 16);
                    self.algorithm_ = algorithm;
                    return _cb(err);
                });
            }
        ], arg:{}}, function(err) {
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
        hash: value, vnodeHashInterval: self.algorithm_.VNODE_HASH_INTERVAL});
    self.db_.get(sprintf('/vnode/%d', vnode), function(err, pnode) {
        if (err) {
            return callback(err);
        }
        self.db_.get(sprintf('/pnode/%s/%d', pnode, vnode), function(_err, data) {
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
    assert.string(data, 'data');

    // first look up the pnode
    db.get(sprintf('/vnode/%d', vnode), function(err, pnode) {
        if (err) {
            return cb(new verror.VError(err, 'unable to add data'));
        }
        log.info({
            vnode: vnode,
            pnode: pnode
        }, 'Consistenthash.addData: vnode maps to');
        // then set the data
        db.put(sprintf('/pnode/%s/%d', pnode, vnode), data, function(_err) {
            if (_err) {
                _err = new verror.VError(_err, 'unable to add data');
            }
            return cb(_err);
        });

        return (undefined);
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
            db.get(sprintf('/pnode/%s', newPnode), function(err) {
                if (err && err.name && err.name === 'NotFoundError') {
                    _.batch = _.batch.put(sprintf('/pnode/%s', newPnode), []);
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
            db.get(sprintf('/vnode/%d', vnode), function(err, pnode) {
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
            db.get(sprintf('/pnode/%s/%d', _.oldPnode, vnode), function(err, d) {
                if (err) {
                    return _cb(err);
                } else {
                    _.data = d;
                    return _cb();
                }
            });
        },
        function delOldMapping(_, _cb) {
            _.batch = _.batch.del(sprintf('pnode/%s/%d', _.oldPnode, vnode));
            db.get(sprintf('/pnode/%s', _.oldPnode), function(err, oldVnodes) {
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
                .put(sprintf('/pnode/%s', _.oldPnode), oldVnodes)
                .del(sprintf('pnode/%s/%d', _.oldPnode, vnode));
                return _cb();
            });
        },
        function addNewMapping(_, _cb) {
            db.get(sprintf('/pnode/%s', newPnode), function(err, vnodes) {
                /*
                 * ignore NotFoundErrors if the pnode is new, since it hasn't
                 * been created yet
                 */
                if (err && !(_.isNew && err && err.name && err.name === 'NotFoundError')) {
                    return _cb(new verror.VError(err));
                }
                if (_.isNew && !vnodes) {
                    vnodes = [];
                }
                vnodes.push(vnode);
                _.batch = _.batch.put(sprintf('/pnode/%s', newPnode), vnodes)
                .put(sprintf('/pnode/%s/%d', newPnode, vnode), _.data)
                .put(sprintf('/vnode/%d', vnode), newPnode);
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
    db.get(sprintf('/pnode/%s', pnode), function(err, vnodes) {
        if (err) {
            return cb(err);
        }
        log.info({
            pnode: pnode,
            vnodes: vnodes
        }, 'ConsistentHash.getVnodes: exiting');
        return cb(null, vnodes);
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
            db.get(sprintf('/pnode/%s', pnode), function(err, v) {
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
            db.del(sprintf('/pnode/%s', pnode), function(err) {
                if (err) {
                    err = new verror.VError(err, 'unable to remove pnode');
                }
                log.info({
                    err: err,
                    pnode: pnode
                }, 'ConsistentHash.removePnode: exiting');
                return _cb(err);
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
            db.get('vnodeCount', function(err, vnodes) {
                serializedHash.vnodes = vnodes;
                return cb(err);
            });
        },
        function getVnodeToPnodeMaps(_, cb) {
            cb = once(cb);
            var count = serializedHash.vnodes;
            for (var vnode = 0; vnode < serializedHash.vnodes; vnode++) {
                db.get(sprintf('/vnode/%d', vnode),
                       (function(v, err, pnode)
                {
                    if (err) {
                        return cb(new verror.VError(err));
                    }
                    serializedHash.pnodeToVnodeMap[pnode] = {};
                    db.get(sprintf('/pnode/%s/%d', pnode, v),
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
            db.get('algorithm', function(err, algorithm) {
                if (err) {
                    err = new verror.VError(err);
                }
                serializedHash.algorithm = algorithm;
                return cb(err);
            });
        },
        function getVersion(_, cb){
            db.get('version', function(err, version) {
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
/*
 * private functions
 */
