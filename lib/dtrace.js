/**
 * @author <a href="mailto:yjxiao@gmail.com">Yunong J Xiao</a>
 */

var dtrace = require('dtrace-provider');



var DTraceProvider = dtrace.DTraceProvider;

var PROBES = {
    'new-start': [],
    // err, method
    'new-done': ['char *', 'char *'],
    // key
    'getnode-start': ['char *'],
    // key, value, pnode, vnode, data
    'getnode-done': ['char *', 'char *', 'char *', 'char *', 'char *'],
    //
    'serialize-start': [],
    // err
    'serialize-done': ['char *'],
    // vnode, data
    'adddata-start': ['int', 'char *'],
    // err, vnode, data
    'adddata-done': ['char *', 'int', 'char *'],
    // newPnode, vnode
    'remapvnode-start': ['char *', 'int'],
    // err, newPnode, oldPnode, vnode
    'remapvnode-done': ['char *', 'char *', 'char *', 'int'],
    // pnode
    'removepnode-start': ['char *'],
    // err, pnode
    'removepnode-done': ['char *', 'char *']
};
var PROVIDER;



///--- API

module.exports = function exportStaticProvider() {
    if (!PROVIDER) {
        PROVIDER = dtrace.createDTraceProvider('node-fash');

        PROVIDER._fash_probes = {};

        Object.keys(PROBES).forEach(function (p) {
            var args = PROBES[p].splice(0);
            args.unshift(p);

            var probe = PROVIDER.addProbe.apply(PROVIDER, args);
            PROVIDER._fash_probes[p] = probe;
        });

        PROVIDER.enable();
    }

    return (PROVIDER);
}();
