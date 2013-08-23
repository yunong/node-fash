#!/usr/bin/env node

/**
 * Copyright (c) 2013, Yunong J Xiao. All rights reserved.
 *
 * fash.js: CLI tool for node-fash
 */

var bunyan = require('bunyan');
var cmdln = require('cmdln');
var fash = require('../lib/index');
var fs = require('fs');
var sprintf = require('util').format;
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

var Cmdln = cmdln.Cmdln;
var BACKENDS = {
    LEVEL_DB: 'leveldb',
    IN_MEMORY: 'memory'
};

function Fash() {
    Cmdln.call(this, {
        name: 'fash',
        desc: 'fash cmdline tool',
        // Custom options. By default you get -h/--help.
        options: [
            {names: ['help', 'h'], type: 'bool',
                help: 'Print help and exit.'},
                {name: 'version', type: 'bool',
                    help: 'Print version and exit.'}
        ]
    });

    this.log = new bunyan({
        name: 'fash',
        level: process.env.LOG_LEVEL || 'warn'
    });
}
util.inherits(Fash, Cmdln);

Fash.prototype.do_create = function(subcmd, opts, args, callback) {
    var self = this;

    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    if (args.length !== 0 || !opts.v || !opts.p) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback(false));
    }

    var pnodes = opts.p.split(' ');
    switch(opts.b) {
        case BACKENDS.IN_MEMORY:
            opts.b = fash.BACKEND.IN_MEMORY;
        break;
        case BACKENDS.LEVEL_DB:
            opts.b = fash.BACKEND.LEVEL_DB;
        break;
        default:
            opts.b = fash.BACKEND.IN_MEMORY;
        break;
    }
    fash.create({
        log: self.log,
        algorithm: opts.a || 'sha256',
        pnodes: pnodes,
        vnodes: opts.v,
        backend: opts.b,
        location: opts.l
    }, function(err, chash) {
        if (err) {
            console.error(err);
            return callback(false);
        }

        chash.serialize(function(_err, sh) {
            if (_err) {
                console.error(_err);
                return callback(false);
            }
            if (opts.o) {
                console.log(sh);
            }
            return (callback());
        });
        return (undefined);
    });

    return (undefined);
};
Fash.prototype.do_create.options = [{
    names: [ 'v', 'vnode' ],
    type: 'number',
    help: 'number of vnodes'
}, {
    names: [ 'p', 'pnode' ],
    type: 'string',
    help: 'physical node names'
}, {
    names: [ 'a', 'algorithm' ],
    type: 'string',
    help: 'the algorithm to use'
}, {
    names: [ 'b', 'backend' ],
    type: 'string',
    help: 'the backend to use one of (memory, leveldb)'
}, {
    names: [ 'l', 'location' ],
    type: 'string',
    help: 'the (optional) location to store the topology --' +
          ' only applies to the leveldb backend'
}, {
    names: [ 'o', 'output' ],
    type: 'bool',
    help: 'serialize and print out the resulting hash to stdout'
}];
Fash.prototype.do_create.help = (
    'create a consistent hash ring.\n'
    + '\n'
    + 'usage:\n'
    + '     fash create [options] \n'
    + '\n'
    + '{{options}}'
);

Fash.prototype.do_add_data = function(subcmd, opts, args, callback) {
    var self = this;
    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    if (args.length !== 0 || !opts.v || !opts.d) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    var hashOptions = {
        log: self.log
    };
    var hash;
    var constructor;

    vasync.pipeline({funcs: [
        function prepInput(_, cb) {
            if (!opts.b || opts.b === BACKENDS.IN_MEMORY) {
                hashOptions.backend = fash.BACKEND.IN_MEMORY;
                constructor = fash.deserialize;
                if (opts.l) {
                    hashOptions.topology = fs.readFileSync(opts.l, 'utf8');
                    return cb();
                } else {
                    hashOptions.topology = '';
                    process.stdin.resume();
                    process.stdin.setEncoding('utf8');

                    process.stdin.on('data', function(chunk) {
                        hashOptions.topology += chunk;
                    });

                    process.stdin.on('end', function() {
                        return cb();
                    });
                }
            } else if (opts.b === BACKENDS.LEVEL_DB) {
                hashOptions.backend = fash.BACKEND.LEVEL_DB;
                constructor = fash.load;
                if (!opts.l) {
                    this.do_help('help', {}, [subcmd], callback);
                    return (callback());
                } else {
                    hashOptions.location = opts.l;
                    return cb();
                }
            } else {
                console.error('one of %j must be specified if not passing ' +
                              'topology via stdin', BACKENDS);
                return (undefined);
            }
            return (undefined);
        },
        function loadRing(_, cb) {
            hash = constructor(hashOptions, cb);
        },
        function addData(_, cb) {
            var count = 0;
            var vnodes = opts.v.split(' ');
            hash.addData(parseInt(vnodes[count], 10), opts.d, addDataCb);
            function addDataCb(err) {
                if (err) {
                    return cb(err);
                }
                if (++count === vnodes.length) {
                    return cb();
                } else {
                    hash.addData(parseInt(vnodes[count], 10),
                                 opts.d, addDataCb);
                }
                return (undefined);
            }
            return (undefined);
        },
        function printRing(_, cb) {
            hash.serialize(function(_err, sh) {
                if (_err) {
                    return cb(new verror.VError(_err,
                                                'unable to print hash'));
                }
                if (opts.o) {
                    console.log(sh);
                }
                return cb();
            });
        }
    ], arg: {}}, function(err) {
        if (err) {
            console.error(err);
        }
    });
    return (undefined);
};
Fash.prototype.do_add_data.options = [{
    names: [ 'v', 'vnode' ],
    type: 'string',
    help: 'the vnode(s) to add the data to'
}, {
    names: [ 'd', 'data' ],
    type: 'string',
    help: 'the data to add, optional, if empty, removes data from the node'
}, {
    names: [ 'b', 'backend' ],
    type: 'string',
    help: 'the backend to use'
}, {
    names: [ 'l', 'location' ],
    type: 'string',
    help: 'the location of the topology'
}, {
    names: [ 'o', 'output' ],
    type: 'bool',
    help: 'serialize and print out the resulting hash to stdout'
}];
Fash.prototype.do_add_data.help = (
    'add data to a vnode.\n'
    + '\n'
    + 'usage:\n'
    + '     fash add_data [options] \n'
    + '\n'
    + '{{options}}'
);

Fash.prototype.do_remap_vnode = function(subcmd, opts, args, callback) {
    var self = this;
    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    if (args.length !== 0 || !opts.v || !opts.p) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    var hashOptions = {
        log: self.log
    };
    var hash;
    var constructor;

    vasync.pipeline({funcs: [
        function prepInput(_, cb) {
            if (!opts.b || opts.b === BACKENDS.IN_MEMORY) {
                hashOptions.backend = fash.BACKEND.IN_MEMORY;
                constructor = fash.deserialize;
                if (opts.l) {
                    hashOptions.topology = fs.readFileSync(opts.l, 'utf8');
                    return cb();
                } else {
                    hashOptions.topology = '';
                    process.stdin.resume();
                    process.stdin.setEncoding('utf8');

                    process.stdin.on('data', function(chunk) {
                        hashOptions.topology += chunk;
                    });

                    process.stdin.on('end', function() {
                        return cb();
                    });
                }
            } else if (opts.b === BACKENDS.LEVEL_DB) {
                hashOptions.backend = fash.BACKEND.LEVEL_DB;
                constructor = fash.load;
                if (!opts.l) {
                    this.do_help('help', {}, [subcmd], callback);
                    return (callback());
                } else {
                    hashOptions.location = opts.l;
                    return cb();
                }
            } else {
                console.error('one of %j must be specified if not passing ' +
                              'topology via stdin', BACKENDS);
                return (undefined);
            }
            return (undefined);
        },
        function loadRing(_, cb) {
            hash = constructor(hashOptions, cb);
        },
        function remap(_, cb) {
            var count = 0;
            var vnodes = opts.v.split(' ');
            hash.remapVnode(opts.p, parseInt(vnodes[count], 10), remapCb);
            function remapCb(err) {
                if (err) {
                    return cb(err);
                }
                if (++count === vnodes.length) {
                    return cb();
                } else {
                    hash.remapVnode(opts.p, parseInt(vnodes[count], 10),
                                    remapCb);
                }
                return (undefined);
            }
        },
        function printRing(_, cb) {
            hash.serialize(function(_err, sh) {
                if (_err) {
                    return cb(new verror.VError(_err,
                                                'unable to print hash'));
                }
                if (opts.o) {
                    console.log(sh);
                }
                return cb();
            });
        }
    ], arg: {}}, function(err) {
        if (err) {
            console.error(err);
        }
    });
    return (undefined);
};
Fash.prototype.do_remap_vnode.options = [{
    names: [ 'v', 'vnode' ],
    type: 'string',
    help: 'the vnode(s) to remap'
}, {
    names: [ 'b', 'backend' ],
    type: 'string',
    help: 'the backend to use'
}, {
    names: [ 'p', 'pnode' ],
    type: 'string',
    help: 'the pnode to remap the vnode(s) to'
}, {
    names: [ 'l', 'location' ],
    type: 'string',
    help: 'the location of the topology'
}, {
    names: [ 'o', 'output' ],
    type: 'bool',
    help: 'serialize and print out the resulting hash to stdout'
}];
Fash.prototype.do_remap_vnode.help = (
    'remap a vnode to a different pnode.\n'
    + '\n'
    + 'usage:\n'
    + '     fash remap_vnode [options] \n'
    + '\n'
    + '{{options}}'
);

Fash.prototype.do_remove_pnode = function(subcmd, opts, args, callback) {
    var self = this;
    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    if (args.length !== 0 || !opts.p) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    var hashOptions = {
        log: self.log
    };
    var hash;
    var constructor;

    vasync.pipeline({funcs: [
        function prepInput(_, cb) {
            if (!opts.b || opts.b === BACKENDS.IN_MEMORY) {
                hashOptions.backend = fash.BACKEND.IN_MEMORY;
                constructor = fash.deserialize;
                if (opts.l) {
                    hashOptions.topology = fs.readFileSync(opts.l, 'utf8');
                    return cb();
                } else {
                    hashOptions.topology = '';
                    process.stdin.resume();
                    process.stdin.setEncoding('utf8');

                    process.stdin.on('data', function(chunk) {
                        hashOptions.topology += chunk;
                    });

                    process.stdin.on('end', function() {
                        return cb();
                    });
                }
            } else if (opts.b === BACKENDS.LEVEL_DB) {
                hashOptions.backend = fash.BACKEND.LEVEL_DB;
                constructor = fash.load;
                if (!opts.l) {
                    this.do_help('help', {}, [subcmd], callback);
                    return (callback());
                } else {
                    hashOptions.location = opts.l;
                    return cb();
                }
            } else {
                console.error('one of %j must be specified if not passing ' +
                              'topology via stdin', BACKENDS);
                return (undefined);
            }
            return (undefined);
        },
        function loadRing(_, cb) {
            hash = constructor(hashOptions, cb);
        },
        function remove(_, cb) {
            hash.removePnode(opts.p, cb);
        },
        function printRing(_, cb) {
            hash.serialize(function(_err, sh) {
                if (_err) {
                    return cb(new verror.VError(_err,
                                                'unable to print hash'));
                }
                if (opts.o) {
                    console.log(sh);
                }
                return cb();
            });
        }
    ], arg: {}}, function(err) {
        if (err) {
            console.error(err);
        }
    });

    return (undefined);
};
Fash.prototype.do_remove_pnode.options = [{
    names: [ 'l', 'location' ],
    type: 'string',
    help: 'the location of the topology'
}, {
    names: [ 'p', 'pnode' ],
    type: 'string',
    help: 'the pnode to remap the vnode(s) to'
}, {
    names: [ 'b', 'backend' ],
    type: 'string',
    help: 'the backend to use'
}, {
    names: [ 'o', 'output' ],
    type: 'bool',
    help: 'serialize and print out the resulting hash to stdout'
}];
Fash.prototype.do_remove_pnode.help = (
    'remove a pnode'
    + '\n'
    + 'usage:\n'
    + '     fash remove_pnode [options] \n'
    + '\n'
    + '{{options}}'
);

Fash.prototype.do_get_node = function(subcmd, opts, args, callback) {
    var self = this;
    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    if (args.length !== 1) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    var hashOptions = {
        log: self.log
    };
    var hash;
    var constructor;

    vasync.pipeline({funcs: [
        function prepInput(_, cb) {
            if (!opts.b || opts.b === BACKENDS.IN_MEMORY) {
                hashOptions.backend = fash.BACKEND.IN_MEMORY;
                constructor = fash.deserialize;
                if (opts.l) {
                    hashOptions.topology = fs.readFileSync(opts.l, 'utf8');
                    return cb();
                } else {
                    hashOptions.topology = '';
                    process.stdin.resume();
                    process.stdin.setEncoding('utf8');

                    process.stdin.on('data', function(chunk) {
                        hashOptions.topology += chunk;
                    });

                    process.stdin.on('end', function() {
                        return cb();
                    });
                }
            } else if (opts.b === BACKENDS.LEVEL_DB) {
                hashOptions.backend = fash.BACKEND.LEVEL_DB;
                constructor = fash.load;
                if (!opts.l) {
                    this.do_help('help', {}, [subcmd], callback);
                    return (callback());
                } else {
                    hashOptions.location = opts.l;
                    return cb();
                }
            } else {
                console.error('one of %j must be specified if not passing ' +
                              'topology via stdin', BACKENDS);
                return (undefined);
            }
            return (undefined);
        },
        function loadRing(_, cb) {
            hash = constructor(hashOptions, cb);
        },
        function getNode(_, cb) {
            hash.getNode(args[0], function(err, node) {
                if (err) {
                    return cb(err);
                }
                console.log(node);
                return cb();
            });
        }
    ], arg: {}}, function(err) {
        if (err) {
            console.error(err);
        }
    });

    return (undefined);
};
Fash.prototype.do_get_node.options = [{
    names: [ 'l', 'location' ],
    type: 'string',
    help: 'the location of the topology'
}, {
    names: [ 'b', 'backend' ],
    type: 'string',
    help: 'the backend to use'
}];
Fash.prototype.do_get_node.help = (
    'hash a value to its spot on the ring'
    + '\n'
    + 'usage:\n'
    + '     fash get_node [options] value\n'
    + '\n'
    + '{{options}}'
);

Fash.prototype.do_print_hash = function(subcmd, opts, args, callback) {
    var self = this;
    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    var hashOptions = {
        log: self.log
    };
    var hash;
    var constructor;

    vasync.pipeline({funcs: [
        function prepInput(_, cb) {
            if (!opts.b || opts.b === BACKENDS.IN_MEMORY) {
                hashOptions.backend = fash.BACKEND.IN_MEMORY;
                constructor = fash.deserialize;
                if (opts.l) {
                    hashOptions.topology = fs.readFileSync(opts.l, 'utf8');
                    return cb();
                } else {
                    hashOptions.topology = '';
                    process.stdin.resume();
                    process.stdin.setEncoding('utf8');

                    process.stdin.on('data', function(chunk) {
                        hashOptions.topology += chunk;
                    });

                    process.stdin.on('end', function() {
                        return cb();
                    });
                }
            } else if (opts.b === BACKENDS.LEVEL_DB) {
                hashOptions.backend = fash.BACKEND.LEVEL_DB;
                constructor = fash.load;
                if (!opts.l) {
                    this.do_help('help', {}, [subcmd], callback);
                    return (callback());
                } else {
                    hashOptions.location = opts.l;
                    return cb();
                }
            } else {
                console.error('one of %j must be specified if not passing ' +
                              'topology via stdin', BACKENDS);
                return (undefined);
            }
            return (undefined);
        },
        function loadRing(_, cb) {
            hash = constructor(hashOptions, cb);
        },
        function printRing(_, cb) {
            hash.serialize(function(_err, sh) {
                if (_err) {
                    return cb(new verror.VError(_err,
                                                'unable to print hash'));
                }
                console.log(sh);
                return cb();
            });
        }
    ], arg: {}}, function(err) {
        if (err) {
            console.error(err);
        }
    });

    return (undefined);
};
Fash.prototype.do_print_hash.options = [{
    names: [ 'l', 'location' ],
    type: 'string',
    help: 'the location of the topology'
}, {
    names: [ 'b', 'backend' ],
    type: 'string',
    help: 'the backend to use'
}];
Fash.prototype.do_print_hash.help = (
    'print out the hash ring'
    + '\n'
    + 'usage:\n'
    + '     fash print_hash [options] value\n'
    + '\n'
    + '{{options}}'
);

Fash.prototype.do_diff = function (subcmd, opts, args, callback) {
    var self = this;

    if (opts.help || args.length !== 2 ||
        (args[0] === '-' && args[1] === '-')) {

        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    function chooseBackend(b) {
        if (b === BACKENDS.IN_MEMORY) {
            return (fash.BACKEND.IN_MEMORY);
        } else if (b === BACKENDS.LEVEL_DB) {
            return (fash.BACKEND.LEVEL_DB);
        } else {
            console.error('one of %j must be specified if not passing ' +
                          'topology via stdin', BACKENDS);
            return (callback());
        }
    }

    function chooseConstructor(b) {
        if (b === BACKENDS.IN_MEMORY) {
            return (fash.deserialize);
        } else if (b === BACKENDS.LEVEL_DB) {
            return (fash.load);
        } else {
            console.error('one of %j must be specified if not passing ' +
                          'topology via stdin', BACKENDS);
            return (callback());
        }
    }

    function loadHash(hashOptions, l, cb) {
        if (hashOptions.backend === fash.BACKEND.IN_MEMORY) {
            if (l === '-') {
                hashOptions.topology = '';
                process.stdin.resume();
                process.stdin.setEncoding('utf8');

                process.stdin.on('data', function(chunk) {
                    hashOptions.topology += chunk;
                });

                process.stdin.on('end', function() {
                    return (cb(null, hashOptions));
                });
            } else {
                hashOptions.topology = fs.readFileSync(l, 'utf8');
                return (cb(null, hashOptions));
            }
        } else if (hashOptions.backend === fash.BACKEND.LEVEL_DB) {
                hashOptions.location = l;
                return (cb(null, hashOptions));
        } else {
            console.error('one of %j must be specified if not passing ' +
                          'topology via stdin', BACKENDS);
            return (callback());
        }
    }

    var constructor1, constructor2, hash1, hash2;
    var hashOptions1 = { log: self.log };
    var hashOptions2 = { log: self.log };
    var diff = {};

    if (!opts.b) {
        hashOptions1.backend = fash.BACKEND.IN_MEMORY;
        hashOptions2.backend = fash.BACKEND.IN_MEMORY;
        constructor1 = fash.deserialize;
        constructor2 = fash.deserialize;
    } else {
        var i = 0;
        while (i < opts._order.length) {
            if (opts._order[i].key === 'b') {
                hashOptions1.backend = chooseBackend(opts._order[i].value);
                constructor1 = chooseConstructor(opts._order[i].value);
                hashOptions2.backend = chooseBackend(opts._order[i].value);
                constructor2 = chooseConstructor(opts._order[i].value);
                i++;
                break;
            }
            i++;
        }
        while (i < opts._order.length) {
            if (opts._order[i].key === 'b') {
                hashOptions2.backend = chooseBackend(opts._order[i].value);
                constructor2 = chooseConstructor(opts._order[i].value);
                break;
            }
            i++;
        }
    }

    vasync.pipeline({funcs: [
        function loadFirst(_, cb) {
            loadHash(hashOptions1, args[0], function (err, hashOptions) {
                hash1 = constructor1(hashOptions, cb);
            });
        },
        function loadSecond(_, cb) {
            loadHash(hashOptions2, args[1], function (err, hashOptions) {
                hash2 = constructor2(hashOptions, cb);
            });
        },
        function removed(_, cb) {
            hash1.getPnodes(function (err, pnodes) {
                pnodes.forEach(function (pnode) {
                    if (!hash2.pnodeToVnodeMap_[pnode]) {
                        diff[pnode] = diff[pnode] || {};
                        diff[pnode]['removed'] =
                            Object.keys(hash1.pnodeToVnodeMap_[pnode]);
                    } else {
                        vnodes = Object.keys(hash1.pnodeToVnodeMap_[pnode]);
                        for (var j = 0; j < vnodes.length; j++) {
                            if (!hash2.pnodeToVnodeMap_[pnode][vnodes[j]]) {
                                diff[pnode] = diff[pnode] || {};
                                diff[pnode]['removed'] =
                                    diff[pnode]['removed'] || [];
                                diff[pnode]['removed'].push(vnodes[j]);
                            }
                        }
                    }
                });
                return (cb());
            });
        },
        function added(_, cb) {
            hash2.getPnodes(function (err, pnodes) {
                pnodes.forEach(function (pnode) {
                    if (!hash1.pnodeToVnodeMap_[pnode]) {
                        diff[pnode] = diff[pnode] || {};
                        diff[pnode]['added'] =
                            Object.keys(hash2.pnodeToVnodeMap_[pnode]);
                    } else {
                        vnodes = Object.keys(hash2.pnodeToVnodeMap_[pnode]);
                        for (var j = 0; j < vnodes.length; j++) {
                            if (!hash1.pnodeToVnodeMap_[pnode][vnodes[j]]) {
                                diff[pnode] = diff[pnode] || {};
                                diff[pnode]['added'] =
                                    diff[pnode]['added'] || [];
                                diff[pnode]['added'].push(vnodes[j]);
                            }
                        }
                    }
                });
                return (cb());
            });
        },
        function print(_, cb) {
            if (Object.keys(diff).length) {
                console.log(JSON.stringify(diff));
            }
        }
    ]}, function (err) {
        if (err) {
            console.error(err);
        }
    });
};
Fash.prototype.do_diff.options = [{
    names: ['b', 'backend'],
    type: 'string',
    help: 'the backend to use'
}];
Fash.prototype.do_diff.help = (
    'compare two topologies'
    + '\n'
    + 'usage:\n'
    + '     fash diff [options] FILE1 FILE2\n'
    + '     cat ring1.json | fash diff - ring2.json\n'
    + '     fash diff -b leveldb ring1.json ring2.json\n'
    + '\n'
    + '{{options}}'
    + '\n'
    + 'If FILE1 or FILE2 is \'-\', read standard input.\n'
    + 'You can use different backends for each input by specifying -b twice:\n'
    + '    fash diff -b leveldb -b memory leveldbdir ring.json'
    
);

cmdln.main(Fash); // mainline
