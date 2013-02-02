/**
* @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
*/

var ConsistentHash = require('./consistent_hash2');


module.exports = {
        createHash: function createHash(options) {
                return new ConsistentHash(options);
        }
};
