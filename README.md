# fash: consistent hashing library for node.js

This module provides a consistent hashing library. Notably, this module the
ability to **deterministically generate the same hash ring topology across a
set of distributed hosts**. Fash also handles collisions of nodes on the ring
and ensures no two nodes will share the same spot on the ring. Additionally,
fash provides the ability to add, remove, or remap physical nodes on the ring
-- useful when a particular physical node has hit its scaling bottleneck.

# Design

Fash consists of a mapping of a set of fixed virtual nodes (vnodes) -- usually
a large number, say 1000000 -- distributed across the hash ring. It is then
possible to map these virtual nodes to a set of physical nodes (pnodes).  In
practice, pnodes are usuall physical shards or servers in a distributed system.
This gives the flexibility of mutating the hashspace of pnodes and the number
of pnodes by re-mapping the vnode assignments.

# Example

## Boostrapping a New Hash Ring

    var fash = require('fash');
    var Logger = require('bunyan');

    var LOG = new Logger({
        name: 'fash',
        level: 'info'
    });

    var chash = fash.create({
        log: LOG, // optional [bunyan](https://github.com/trentm/node-bunyan) log object.
        algorithm: fash.ALGORITHMS.SHA256, // Can be any algorithm supported by openssl.
        pnodes: ['A', 'B', 'C', 'D', 'E'], // The set of physical nodes to insert into the ring.
        vnodes: 1000000 // The virtual nodes to place onto the ring. Once set, this can't be changed for the lifetime of the ring.
    });

    var node = chash.getNode('someKeyToHash');
    console.log('key hashes to pnode', node);

If the config used to bootstrap fash is the same across all clients, then the
ring toplogy will be the same as well. By default, fash will evenly distribute
vnodes across the set of pnodes. If you wish to have a custom mapping of pnodes
to vnodes, see the later section on serialization.

## Remapping Pnodes in the Ring
Fash gives you the ability to add and rebalance the pnodes in the ring by using
the remapNode() function, which will emit an 'update' event when done, 'error'
event on error, and also return an optional callback.

You can also remove pnodes from the ring, but **you must first rebalance the
ring by reassigning its vnodes to other pnodes** via remapVnode(). Then you can
invoke removeNode(), which will emit an 'update' event when done, an 'error'
even on error, and also return an optional callback.

You can assign an arbitrary number of vnodes to the new vnode -- also -- the
pnode can be a new node, or an existing one.  Again, as long as the order of
removes and remaps is consistent across all clients, the ring toplogy will be
consistent as well.

    var fash = require('fash');
    var Logger = require('bunyan');

    var LOG = new Logger({
        name: 'fash',
        level: 'info'
    });

    var chash = fash.create({
        log: LOG,
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: 100000
    });

    // get vnodes from A
    var aVnodes = chash.getVnodes('A');
    aVnodes = aVnodes.slice(aVnodes.length / 2);

    // remap some of A's vnodes to B
    chash.remapVnode('B', aVnodes, function(err, ring, pnodes) {
        console.log('new ring topology', ring);
        console.log('changed pnode->vnode mappings', pnodes);
    });

## Adding More Pnodes to the Ring
You can add additional pnodes to the ring after fash has been initialized by
invoking remapVnode(). Which will emit an 'update' event when done, 'error'
event if there's an error, and also optionally returns a callback.

    var fash = require('fash');
    var Logger = require('bunyan');

    var LOG = new Logger({
        name: 'fash',
        level: 'info'
    });

    var chash = fash.create({
        log: LOG,
        algorithm: fash.ALGORITHMS.SHA256,
        pnodes: ['A', 'B', 'C', 'D', 'E'],
        vnodes: 100000
    });
    // specify the set of virtual nodes to assign to the new physical node.
    var vnodes = [0, 1, 2, 3, 4];

    /* wait for an update event, returns the current topology of the ring, as
     * well as a map of the removed pnode->vnode[] mappings.
     */
    chash.once('update', function(ring, removedMapping) {
        console.log('ring topology updated', ring);
        console.log('removed mappings', removedMapping);
    });

    // add the physical node 'F' to the ring.
    chash.remapVnode('F', vnodes);

Fash will remove the vnodes from their previously mapped physical nodes, and
map them to the new pnode.

## Removing Pnodes from the Ring
You can remove physical nodes from the ring by first remapping the pnode's
vnodes to another pnode, and then removing the pnode.

    var fash = require('fash');
    var Logger = require('bunyan');

    var LOG = new Logger({
      name: 'fash',
      level: 'info'
    });

    var chash = fash.create({
            log: LOG,
            algorithm: fash.ALGORITHMS.SHA256,
            pnodes: ['A', 'B', 'C', 'D', 'E'],
            vnodes: 10000
    });

    // get the vnodes that map to B
    var vnodes = chash.getVnodes('B');
    // rebalance them to A
    chash.remapVnode('A', vnodes, function(err, ring, removedMap) {
        // remove B
        chash.removePnode('B', function(err, ring, pnode) {
            if (!err) {
                console.log('removed pnode %s', pnode);
            }
        });
    });

## Serializing and Persisting the Ring Toplogy
At any time, the ring toplogy can be accessed by:

    chash.serialize();

Which returns the ring topology, which is a JSON serialized object string which
looks like

    {
        pnodeToVnodeMap: {
            A: {0, 1, 2},
            ...
        }, // the pnode to vnode mapings.
        vnode // the total number of vnodes in the ring.
    }

Additionally, anytime remapVnode() is invoked, fash emits an `update` event
which contains an updated version of this object that is **not** JSON
serialized. Fash can be instantiated given a topology object instead of a list
of nodes. This also allows you to specify a custom pnode to vnode topology --
as mentioned in the earlier bootstrapping section.

    var fash = require('fash');
    var Logger = require('bunyan');

    var LOG = new Logger({
        name: 'fash',
        level: 'info'
    });

    var chash = fash.create({
       log: LOG,
       algorithm: fash.ALGORITHMS.SHA256,
       pnodes: ['A', 'B', 'C', 'D', 'E'],
       vnodes: 10000
    });

    var topology = chash.serialize();

    var chash2 = fash.deserialize({
        log: LOG,
        topology: topology
    });

That's it, chash and chash2 now contain the same ring toplogy.

Copyright (c) 2013 Yunong Xiao

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
