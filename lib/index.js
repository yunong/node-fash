/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */

var assert = require('assert-plus');

var ConsistentHash = require('./consistent_hash');


module.exports = {
    create: function create(options) {
        assert.object(options, 'options');
        assert.object(options.algorithm, 'options.algorithm');
        assert.string(options.algorithm.algorithm,
                      'options.algorithm.algorithm');
        assert.string(options.algorithm.max, 'options.algorithm.max');
        assert.number(options.vnodes, 'options.vnodes');
        assert.arrayOfString(options.pnodes, 'options.pnodes');
        return new ConsistentHash(options);
    },
    deserialize: function deserialize(options) {
        assert.object(options, 'options');
        assert.string(options.topology, 'options.topology');
        options.topology = JSON.parse(options.topology);

        assert.object(options.topology.pnodeToVnodeMap,
                      'options.topology.pnodeToVnodeMap');
        assert.number(options.topology.vnodes, 'options.topology.vnodes');
        assert.object(options.topology.algorithm, 'options.topology.algorithm');
        assert.string(options.topology.algorithm.algorithm,
                      'options.topology.algorithm.algorithm');
        assert.string(options.topology.algorithm.max,
                      'options.topology.algorithm.max');
        options.algorithm = options.topology.algorithm;
        return new ConsistentHash(options);
    },
    ALGORITHMS: {
        SHA256: {
            algorithm: 'sha256',
            max: 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' +
                 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF'
        }
    }
};
