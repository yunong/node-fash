/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */

var assert = require('assert-plus');
var bignum = require('bignum');
var bunyan = require('bunyan');
var common = require('./common');
var fash = require('./index');
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
 * @param [Object] options.log The optional Bunyan log object.
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
function ConsistentHash(options) {
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
    self.db_ = levelup(options.location, {
        //createIfMissing: false,
        createIfMissing: true,
        compression: false,
        cacheSize: 800 * 1024 * 1024,
        keyEncoding: 'utf8',
        valueEncoding: 'utf8'
    });

    /**
     * the management leveldb
     */
    self.manDb_ = levelup(options.location + '_man', {
        createIfMissing: true,
        compression: false,
        cacheSize: 800 * 1024 * 1024,
        keyEncoding: 'utf8',
        valueEncoding: 'utf8'
    });

    /**
     * 1) create allvnodes key which keeps track of the # of vnodes.
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
    function createNewRing() {
        log.info('instantiating new ring from scratch.');
        var tasks = [
            // step 1
            function putVnodeCount(_, cb) {
                _.batch = _.put('allvnodes', self.vnodeCount_);
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
                        pnodeToVnodeMap[pnode] = [];
                    }
                    pnodeToVnodeMap[pnode].push(vnode);

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
                    _.batch = _batch.put(sprintf('/pnode/%s', pnode),
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

        vasync.pipeline({func: tasks, arg: {}}, function(err) {
            if (err) {
                throw new verror.VError('unable to create ring', err);
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
        createNewRing();
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
                                     self.db_.get(sprintf('/pnode/%s/%d', pnode, vnode),
                                                  function(err, data) {
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
        vnode; vnode,
        data: data
    }, 'Consistenthash.addData: entering');

    // first look up the pnode
    db.get(sprintf('/vnode/%d', vnode), function(err, pnode) {
        if (err) {
            return cb(err);
        }
        log.info({
            vnode: vnode,
            pnode: pnode
        }, 'Consistenthash.addData: vnode maps to');
        // then set the data
        db.put(sprintf('/pnode/%s/%d', pnode, vnode), data, cb);
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

    // assert the vnodes, ensuring that:
    // 1) vnode actually exists.
    // 2) vnode doesn't already belong to the newPnode.
    if ((vnode > self.vnodeCount_) || (vnode < 0)) {
        return cb(new verror.VError('vnode ' + v +
                                    ' does not exist in the ring'));
    }

    var batch = db.batch().put(sprintf('/pnode/%s', newPnode), 1);
    // get the previous vnode to pnode mapping
    db.get(sprintf('/vnode/%d', vnode), function(err, pnode) {
        if (err) {
            return cb(err);
        }
        // check that the vnode doesn't already belong to the newPnode.
        if (pnode === newPnode) {
            return (new verror.VError('vnode ' + vnode +
                                      ' already belongs to pnode'));
        }
        // get the data of the vnode
        db.get(sprintf('/pnode/%s/%d', pnode, vnode), function(err, d) {
            if (err) {
                return cb(err);
            }
            // delete the previous pnode mapping and put in the new mapping
            batch = batch.put(sprintf('/pnode/%s/%d', newPnin ode, vnode), d)
            .del('pnode/%s/%d', pnode, vnode);
            batch.write(cb);
        });
    });
};

/**
 * Removes a pnode from the hash ring.  Note the pnode must not map to any
 * vnodes.  Remove the vnodes first by re-assigning them to other pnodes before
 * invoking this function.
 *
 * @param {String} pnode The pnode to remove.
 * @param {function} cb The optional callback f(err).
 */
ConsistentHash.prototype.removePnode = function removePnode(pnode, cb) {
    var self = this;
    var log = self.log;
    var db = self.db_;

    log.info({
        pnode: pnode
    }, 'ConsistentHash.removePnode: entering');

    assert.string(pnode, 'pnode');
    // check that the pnode exists
    db.get(sprintf('/pnode/%s', pnode), function(err, pnode) {
        if (err) {
            return cb(err);
        }
    });

    if (!vnodes) {
        var msg = sprintf('pnode %s not in ring, skipping', pnode);
        throw new verror.VError(msg);
    }

    if (Object.keys(vnodes).length > 0) {
        var errMsg = 'pnode still maps to vnodes, re-assign vnodes first';
        throw new verror.VError(errMsg);
    }

    // remove references to pnode.
    self.pnodes_.splice(self.pnodes_.indexOf(pnode), 1);
    self.pnodeToVnodeMap_[pnode] = null;
    delete self.pnodeToVnodeMap_[pnode];

    log.info({
        pnode: pnode
    }, 'ConsistentHash.removePnode: exiting');

    if (cb) {
        var newTopology = JSON.parse(self.serialize());
        return cb(newTopology, pnode);
    }

    return (undefined);
};

/*
 * private functions
 */
