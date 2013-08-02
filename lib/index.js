/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */

var assert = require('assert-plus');
var sprintf = require('util').format;
var verror = require('verror');

module.exports = {
    create: function create(options) {
        assert.object(options, 'options');
        assert.string(options.algorithm, 'options.algorithm');
        options.algorithm = getAlgorithm(options.algorithm);
        assert.number(options.vnodes, 'options.vnodes');
        assert.arrayOfString(options.pnodes, 'options.pnodes');
        assert.string(options.backend, 'options.backend');
        var backend = require(options.backend);
        return new backend.ConsistentHash(options);
    },
    deserialize: function deserialize(options) {
        assert.object(options, 'options');
        assert.string(options.backend, 'options.backend');
        var backend = require(options.backend);
        assert.string(options.topology, 'options.topology');
        options.topology = JSON.parse(options.topology);
        assert.object(options.topology.pnodeToVnodeMap,
                      'options.topology.pnodeToVnodeMap');
        assert.number(options.topology.vnodes, 'options.topology.vnodes');
        assert.string(options.topology.algorithm, 'options.topology.algorithm');
        assertVersion(options.topology.version);
        options.algorithm = getAlgorithm(options.topology.algorithm);
        return new backend.ConsistentHash(options);
    },
    ALGORITHMS: {
        SHA256: {
            NAME: 'sha256',
            MAX: 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' +
                 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF'
        },
        SHA1: {
            NAME: 'sha1',
            MAX: 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' +
                 'FFFFFFFF'
        },
        MD5: {
            NAME: 'md5',
            MAX: 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF' + 'FFFFFFFF'
        }
    },
    BACKEND: {
        IN_MEMORY: './backend/in_memory',
        LEVEL_DB: './backend/leveldb'
    },
    VERSION: '1.2.0'
};

/**
 * disallow newer versions
 */
function assertVersion(version) {
    assert.string(version, 'options.topology.version');
    var FASH_VERSION = module.exports.VERSION.split('.');
    var versionArray = version.split('.');

    var msg = sprintf('version %s is not compatible with current ' +
                      'version %s', version, FASH_VERSION);
    if (versionArray.length > FASH_VERSION.length) {
        throw new verror.VError(msg);
    }
    for (var i = 0; i < FASH_VERSION.length; i++) {
        var fVersion = parseInt(FASH_VERSION[i], 10);
        var v;
        if (versionArray.length > i) {
            v = parseInt(versionArray[i], 10);
        }

        if (fVersion < v) {
            throw new verror.VError(msg);
        } else {
            continue;
        }
    }
}

function getAlgorithm(algo) {
    switch (algo) {
        case 'sha256':
        case 'sha-256':
        case 'SHA256':
        case 'SHA-256':
            return module.exports.ALGORITHMS.SHA256;
        case 'sha1':
        case 'sha-1':
        case 'SHA1':
        case 'SHA-1':
            return module.exports.ALGORITHMS.SHA1;
        case 'md5':
        case 'MD5':
            return module.exports.ALGORITHMS.MD5;
        default:
            throw new verror.VError('algorithm %s is not supported', algo);
    }
}
