var fash = require('../lib');
var fs = require('fs');
var Logger = require('bunyan');
var restify = require('restify');
var verror = require('verror');

var LOG = new Logger({
    name: 'fash-load-test',
    level: process.env.LOG_LEVEL || 'warn'
});

var DB_LOCATION = process.env.DB_LOCATION || '/tmp/fash-db';
var FASH_BACKEND = process.env.FASH_BACKEND || fash.BACKEND.LEVEL_DB;
var LVL_CFG = {
    createIfMissing: true,
    errorIfExists: false,
    compression: false,
    cacheSize: 800 * 1024 * 1024
};

var RING = fash.load({
    log: LOG,
    backend: fash.BACKEND.LEVEL_DB,
    location: DB_LOCATION,
    leveldbCfg: LVL_CFG
}, function (err) {
    if (err) {
        throw new verror.VError(err, 'unable to load ring from disk');
    }
});

//console.log('loading ring into memory');
//var RING = fash.deserialize({
    //log: LOG,
    //topology: fs.readFileSync(DB_LOCATION, 'utf-8'),
    //backend: fash.BACKEND.IN_MEMORY,
//}, function (err) {
    //if (err) {
        //throw new verror.VError(err, 'unable to load ring from disk');
    //}

    //console.log('ring loaded');
//});

var server = restify.createServer();
server.use(restify.bodyParser());
server.post('/hash', function (req, res, next) {
    RING.getNode(req.params.key, function (err, val) {
        if (err) {
            LOG.error({err: err, key: req.params.key}, 'unable to hash key');
            return next(err);
        } else {
            LOG.info({key: req.params.key, val: val}, 'finished hash');
            res.send();
            return next();
        }
    });
});

server.listen(12345, function () {
    console.log('server started.');
});
