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
    self.vnodeHashInterval_ = self.algorithmMax_.div(self.vnodesBignum_);

    /**
     * The String array of physical nodes in the ring.
     */
    self.pnodes_ = options.pnodes ? options.pnodes.slice() : [];
    self.pnodes_.sort();

    /**
     * the datapath leveldb
     */
    self.db_;
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
        var tasks = [
            function openDb(_, cb) {
                levelup(options.location, {
                    createIfMissing: true,
                    compression: false,
                    cacheSize: 800 * 1024 * 1024,
                    keyEncoding: 'utf8',
                    valueEncoding: 'json'
                }, function(err, db) {
                    if (err) {
                        return cb(err);
                    }
                    self.db_ = db;
                    _.batch = db.batch();
                    return cb();
                });
            },
            // step 1
            function putVnodeCount(_, cb) {
                _.batch = _.batch.put('vnodeCount', self.vnodeCount_);
                return cb();
            },
            /**
             * steps 2, 3
             * Allocate the vnodes to the pnodes by
             * vnode % total_pnode = assigned pnode.
             */
            function allocateVnodes(_, cb) {
                _.pnodeToVnodeMap = {};
                for (var vnode = 0; vnode < self.vnodeCount_; vnode++) {
                    var pnode = self.pnodes_[vnode % self.pnodes_.length];
                    var hashspace = common.findHashspace({
                        vnode: vnode,
                        vnodeHashInterval: self.vnodeHashInterval_,
                        log: log
                    });

                    log.debug({
                        hashspace: hashspace,
                        vnode: vnode,
                        pnode: pnode
                    }, 'ConsistentHash.new: assigning hashspace to vnode to pnode');

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
                return cb();
            },
            // step 4
            function writePnodeKeys(_, cb) {
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
                return cb();
            },
            function writeMetadata(_, cb) {
                // step 5
                _.batch = _.batch.put('algorithm', JSON.stringify(self.algorithm_));
                // step 6
                _.batch = _.batch.put('version', fash.VERSION);
                // step 7
                _.batch = _.batch.put('complete', 1);
                return cb();
            },
            function commit(_, cb) {
                _.batch.write(cb);
            }
        ];

        vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
            if (err) {
                return callback(new verror.VError(err, 'unable to create ring'));
            } else {
                log.info('successfully instantiated new ring');
                return callback();
            }
        });
    }
    if (options.topology) {
        var topology = options.topology;
        log.info('ConsistentHash.new: deserializing an already existing ring.');
        //log.debug({
        //topology: topology
        //}, 'ConsistentHash.new: previous topology');
        //self.pnodeToVnodeMap_ = topology.pnodeToVnodeMap;
        //var pnodeKeys = Object.keys(self.pnodeToVnodeMap_);

        //pnodeKeys.forEach(function(pnode) {
        //self.db_.put('/pnode/' + pnode, topolog.pnodeToVnodeMap[pnode]);
        //});

        log.info('ConsistentHash.new: finished deserializing');
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
    var vnode = common.findVnode({hash: value,
                                 vnodeHashInterval: self.vnodeHashInterval_});
    self.db_.get(sprintf('/vnode/%d', vnode), function(err, pnode) {
        if (err) {
            return callback(err);
        }
        self.db_.get(sprintf('/pnode/%s/%d', pnode, vnode), function(err, data) {
            if (err) {
                return callback(err);
            }
            return callback(null, {pnode: pnode, vnode: vnode, data: data});
        });
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
        db.put(sprintf('/pnode/%s/%d', pnode, vnode), data, function(err) {
            if (err) {
                err = new verror.VError(err, 'unable to add data');
            }
            return cb(err);
        });
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
        function assertVnodeExists(_, cb) {
            if ((vnode > self.vnodeCount_) || (vnode < 0)) {
                return cb(new verror.VError('vnode ' + v +
                                            ' does not exist in the ring'));
            } else {
                return cb();
            }
        },
        function initBatch(_, cb) {
            _.batch = db.batch();
            return cb();
        },
        function checkAndCreateNewPnode(_, cb) {
            db.get(sprintf('/pnode/%s', newPnode), function(err) {
                if (err && err.name && err.name === 'NotFoundError') {
                    _.batch = _.batch.put(sprintf('/pnode/%s', newPnode), []);
                    _.isNew = true;
                    return cb();
                } else {
                    if (err) {
                        return cb(new verror.VError(err));
                    } else {
                        return cb();
                    }
                }
            });
        },
        function getOldVnodeMapping(_, cb) {
            // get the previous vnode to pnode mapping
            db.get(sprintf('/vnode/%d', vnode), function(err, pnode) {
                if (err) {
                    return cb(new verror.VError(err));
                }
                _.oldPnode = pnode;
                // check that the vnode doesn't already belong to the newPnode.
                if (pnode === newPnode) {
                    return cb(new verror.VError('vnode ' + vnode +
                                                ' already belongs to pnode'));
                } else {
                    return cb();
                }
            });
        },
        function getVnodeData(_, cb) {
            db.get(sprintf('/pnode/%s/%d', _.oldPnode, vnode), function(err, d) {
                if (err) {
                    return cb(err);
                } else {
                    _.data = d;
                    return cb();
                }
            });
        },
        function delOldMapping(_, cb) {
            _.batch = _.batch.del(sprintf('pnode/%s/%d', _.oldPnode, vnode));
            db.get(sprintf('/pnode/%s', _.oldPnode), function(err, oldVnodes) {
                if (err) {
                    return cb(new verror.VError(err,
                                                'couldn\'t get path /pnode/' +
                                                 _.oldPnode));
                }
                var idx = oldVnodes.indexOf(vnode);
                if (idx === -1) {
                    return cb(new verror.VError('vnode: ' + vnode +
                                                ' does not ' +
                                                'exist in old pnode: ' +
                                                _.oldPnode));
                }
                oldVnodes.splice(idx, 1);
                _.batch = _.batch
                .put(sprintf('/pnode/%s', _.oldPnode), oldVnodes)
                .del(sprintf('pnode/%s/%d', _.oldPnode, vnode));
                return cb();
            });
        },
        function addNewMapping(_, cb) {
            db.get(sprintf('/pnode/%s', newPnode), function(err, vnodes) {
                /*
                 * ignore NotFoundErrors if the pnode is new, since it hasn't
                 * been created yet
                 */
                if (err && !(_.isNew && err && err.name && err.name === 'NotFoundError')) {
                    return cb(new verror.VError(err));
                }
                if (_.isNew && !vnodes) {
                    vnodes = [];
                }
                vnodes.push(vnode);
                _.batch = _.batch.put(sprintf('/pnode/%s', newPnode), vnodes)
                .put(sprintf('/pnode/%s/%d', newPnode, vnode), _.data)
                .put(sprintf('/vnode/%d', vnode), newPnode);
                return cb();
            });
        },
        function commit(_, cb) {
            _.batch.write(function(err) {
                if (err) {
                    return cb(new verror.VError(err));
                } else {
                    return cb();
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
    var vnodes;
    vasync.pipeline({funcs: [
        function checkPnodeExists(_, cb){
            db.get(sprintf('/pnode/%s', pnode), function(err, v) {
                if (err) {
                    return cb(new verror.VError(err, 'pnode does not exist'));
                }
                _.vnodes = v;
                return cb();
            });
        },
        function checkPnodeHasVnodes(_, cb) {
            if (_.vnodes && _.vnodes.length > 0) {
                var errMsg = 'pnode still maps to vnodes, ' +
                             're-assign vnodes first';
                return cb(new verror.VError(errMsg));
            }
            return cb();
        },
        function removePnode(_, cb) {
            // remove /pnode/%s
            db.del(sprintf('/pnode/%s', pnode), function(err) {
                if (err) {
                    err = new verror.VError(err, 'unable to remove pnode');
                }
                log.info({
                    err: err,
                    pnode: pnode
                }, 'ConsistentHash.removePnode: exiting');
                return cb(err);
            })
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
ConsistentHash.prototype.serialize = function serialize() {
    var self = this;
    var log = self.log;
    var db = self.db_;
    log.info('ConsistentHash.serialize: entering');
    var serializedHash = {
        vnodes: null,
        pnodeToVnodeMap: {},
        algorithm: null,
        version: null
    };

    var tasks = [
        function getNumberOfVnodes(_, cb) {
            db.get('vnodeCount', function(err, vnodes) {
                serializedHash.vnodeCount = vnodes;
                return cb(err);
            });
        },
        function getVnodeToPnodeMaps(_, cb) {
            var count = 0;
            for (var vnode = 0; vnode < _.allvnodes; vnode++) {
                db.get(sprintf('/vnode/%d', vnode), function(err, pnode) {
                    if (err) {
                        return cb(err);
                    }
                    db.get(sprintf('/pnode/%s/%d', pnode, vnode), function(err, data) {
                        if (err) {
                            return cb(err);
                        }
                        serializedHash.pnodeToVnodeMap[pnode][vnode] = data;
                        if ((++count) === _.allvnodes - 1) {
                            return cb();
                        }
                    });
                });
            }
        },
        function getAlgorithm(_, cb){
            db.get('algorithm', function(err, algorithm) {
                serializedHash.algorithm = algorithm;
                return cb(err);
            });
        },
        function getVersion(_, cb){
            db.get('version', function(err, version) {
                serializedHash.version = version;
                return cb(err);
            });
        }
    ];

    vasync.pipeline({funcs: tasks, arg: {}}, function(err) {
        if (err) {
            return callback(new verror.VError('unable to serialize ring', err));
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
