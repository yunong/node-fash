/**
* @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
*/

var assert = require('assert-plus');

var ConsistentHash = require('./consistent_hash');


module.exports = {
        create: function create(options) {
                assert.object(options, 'options');
                assert.string(options.algorithm, 'options.algorithm');
                assert.number(options.vnodes, 'options.vnodes');
                assert.arrayOfString(options.pnodes, 'options.pnodes');
                assert.optionalBool(options.random, 'options.random');
                return new ConsistentHash(options);
        },
        deserialize: function deserialize(options) {
                assert.object(options, 'options');
                assert.string(options.algorithm, 'options.algorithm');
                assert.arrayOfObject(options.topology, 'options.topology');
                return new ConsistentHash(options);
        }
};
