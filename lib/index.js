/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */

var assert = require('assert-plus');

var ConsistentHash = require('./consistent_hash');


module.exports = {
    create: function create(options) {
        assert.object(options, 'options');
        assert.string(options.algorithm, 'options.algorithm');
        assert.string(options.algorithmMax, 'options.algorithmMax');
        assert.number(options.vnodes, 'options.vnodes');
        assert.arrayOfString(options.pnodes, 'options.pnodes');
        return new ConsistentHash(options);
    },
    deserialize: function deserialize(options) {
        assert.object(options, 'options');
        assert.string(options.algorithm, 'options.algorithm');
        assert.string(options.algorithmMax, 'options.algorithmMax');
        assert.string(options.topology, 'options.topology');
        options.topology = JSON.parse(options.topology);
        assert.object(options.topology.pnodeToVnodeMap,
                      'options.topology.pnodeToVnodeMap');
        assert.number(options.topology.vnodes, 'options.topology.vnodes');
        return new ConsistentHash(options);
    },
    SHA_256_MAX: 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' +
                 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF',
};
