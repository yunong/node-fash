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
var util = require('util');

var Cmdln = cmdln.Cmdln;

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
        return (callback());
    }

    var pnodes = opts.p.split(' ');
    switch(opts.b) {
        case 'memory':
            opts.b = fash.BACKEND.IN_MEMORY;
        break;
        case 'leveldb':
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
        }

        chash.serialize(function(err, sh) {
            if (err) {
                console.error(err);
            }
            console.log(sh);
            return (callback());
        });
    });
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
    help: 'the backend to use'
}, {
    names: [ 'l', 'location' ],
    type: 'string',
    help: 'the (optional) location to store the topology --' +
          ' only applies to non-inmemory backends'
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
    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    if (args.length !== 0 || !opts.v || !opts.f) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    var topology = fs.readFileSync(opts.f, 'utf8');
    var chash = fash.deserialize({topology: topology});
    var vnodes = opts.v.split(' ');
    vnodes.forEach(function(vnode, index) {
        chash.addData(parseInt(vnode, 10), opts.d);
        if (index === vnodes.length - 1) {
            console.log(chash.serialize());
        }
    });

    return (undefined);
};
Fash.prototype.do_add_data.options = [{
    names: [ 'f', 'topology' ],
    type: 'string',
    help: 'the topology to modify'
}, {
    names: [ 'v', 'vnode' ],
    type: 'string',
    help: 'the vnode(s) to add the data to'
}, {
    names: [ 'd', 'data' ],
    type: 'string',
    help: 'the data to add, optional, if empty, removes data from the node'
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
    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    if (args.length !== 0 || !opts.v || !opts.f || !opts.p) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    var topology = fs.readFileSync(opts.f, 'utf8');
    var chash = fash.deserialize({topology: topology});
    var vnodes = opts.v.split(' ');
    for (var i = 0; i < vnodes.length; i++) {
        vnodes[i] = parseInt(vnodes[i], 10);
    }

    chash.remapVnode(opts.p, vnodes);
    console.log(chash.serialize());

    return (undefined);
};
Fash.prototype.do_remap_vnode.options = [{
    names: [ 'f', 'topology' ],
    type: 'string',
    help: 'the topology to modify'
}, {
    names: [ 'v', 'vnode' ],
    type: 'string',
    help: 'the vnode(s) to remap'
}, {
    names: [ 'p', 'pnode' ],
    type: 'string',
    help: 'the pnode to remap the vnode(s) to'
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
    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    if (args.length !== 0 || !opts.f || !opts.p) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    var topology = fs.readFileSync(opts.f, 'utf8');
    var chash = fash.deserialize({topology: topology});

    chash.removePnode(opts.p);
    console.log(chash.serialize());

    return (undefined);
};
Fash.prototype.do_remove_pnode.options = [{
    names: [ 'f', 'topology' ],
    type: 'string',
    help: 'the topology to modify'
}, {
    names: [ 'p', 'pnode' ],
    type: 'string',
    help: 'the pnode to remap the vnode(s) to'
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
    if (opts.help) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    if (args.length !== 1 || !opts.f) {
        this.do_help('help', {}, [subcmd], callback);
        return (callback());
    }

    var topology = fs.readFileSync(opts.f, 'utf8');
    var chash = fash.deserialize({topology: topology});
    console.log(chash.getNode(args[0]));

    return (undefined);
};
Fash.prototype.do_get_node.options = [{
    names: [ 'f', 'topology' ],
    type: 'string',
    help: 'the hash ring topology'
}];
Fash.prototype.do_get_node.help = (
    'hash a value to its spot on the ring'
    + '\n'
    + 'usage:\n'
    + '     fash get_node [options] value\n'
    + '\n'
    + '{{options}}'
);

cmdln.main(Fash); // mainline
