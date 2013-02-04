# fash: consistent hashing library for node.js

This module provides a consistent hashing library. Notably, this module the
ability to **deterministically generate the same hash ring topology across a
set of distributed hosts**. Fash also handles collisions of nodes on the ring
and ensures no two nodes will share the same spot on the ring. Additionally,
fash provides the ability to add, remove, or remap physical nodes on the ring
-- useful when a particular physical node has hit its scaling bottleneck.

# Design

Fash consists of a mapping of a set of fixed virtual nodes (vnodes) -- usually
a large number, say 100000 -- distributed across the hash ring. It is then
possible to map these virtual nodes to a set of physical nodes (pnodes).  In
practice, pnodes are usuall physical shards or servers in a distributed system.
This gives the flexibility of mutating the hashspace of pnodes and the number
of pnodes by re-mapping the vnode assignments.

# Example

## Boostrapping a New Hash Ring

    var fash = require('fash');
    var Logger = require('bunyan');

    var log = new Logger({
      name: 'fash',
      level: 'info'
    });

    var chash = fash.create({
      log: log, // optional [bunyan](https://github.com/trentm/node-bunyan) log object.
      algorithm: 'sha256', // Can be any algorithm supported by openssl.
      pnodes: ['A', 'B', 'C', 'D', 'E'], // The set of physical nodes to insert into the ring.
      vnodes: 1000, // The virtual nodes to place onto the ring. Once set, this can't be changed for the lifetime of the ring.
      random: true // Whether to placce the vnodes randomly or deterministically onto the ring.
    });

    var node = chash.getNode('someKeyToHash');

If the config used to bootstrap fash is the same across all clients, then the
ring toplogy will be the same as well, unless `random: true` is set.

## Remapping Pnodes in the Ring
Fash gives you the ability to add and rebalance the pnodes in the ring by using
the remapNode() function, which will emit an 'update' event when done, 'error'
event on error, and also return an optional callback.

You can also remove pnodes from the ring, but **you must first rebalance the ring
by reassigning its vnodes to other pnodes** via remapNode(). Then you can invoke
removeNode(), which will emit an 'update' event when done, an 'error' even on
error, and also return an optional callback.

You can assign an arbitrary number of vnodes to the new vnode -- also -- the
pnode can be a new node, or an existing one.  Again, as long as the order of
removes and remaps is consistent across all clients, and 'random: false', the
ring toplogy will be consistent as well.

## Adding More Pnodes to the Ring
You can add additional nodes to the ring after fash has been initialized by
invoking remapNode(). Which will emit an 'update' event when done, 'error' event
if there's an error, and also optionally returns a callback.

    var fash = require('fash');
    var Logger = require('bunyan');

    var log = new Logger({
      name: 'fash',
      level: 'info'
    });

    var chash = fash.create({
            log: LOG,
            algorithm: 'sha256',
            pnodes: ['A', 'B', 'C', 'D', 'E'],
            vnodes: numberOfVnodes,
            random: true
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
    chash.remapNode('F', vnodes);

Fash will remove the vnodes from their previously mapped physical nodes, and
map them to the new pnode.

## Removing Pnodes from the Ring

    var fash = require('fash');
    var Logger = require('bunyan');

    var log = new Logger({
      name: 'fash',
      level: 'info'
    });

    var chash = fash.create({
            log: LOG,
            algorithm: 'sha256',
            pnodes: ['A', 'B', 'C', 'D', 'E'],
            vnodes: numberOfVnodes,
            random: true
    });

    // get the vnodes that map to B
    var vnodes = chash.getVnodes('B');
    // rebalance them to A
    chash.remapNode('A', Object.keys(vnodes), function(err, ring, removedMap) {
        // remove B
        chash.removeNode('B', function(err, ring, pnode) {
            if (!err) {
                console.log('removed pnode %s', pnode);
            }
        });
    });

## Persisting Ring Toplogy

At any time, the ring toplogy can be accessed by:

    chash.serialize();

Which returns the ring topology, which is an array of

    {
      hashspace, // the hashspace in a hex string that this node occupies.
      pnode, // the phsyical node name.
      vnode // the vnode it maps to.
    }

The array is sorted by hashspace. Additionally, anytime remapNode() is invoked,
fash emits an `update` event which contains an updated version of the ring
array. Fash can be instantiated given a topology object instead of a
list of nodes.

    var fash = require('fash');
    var Logger = require('bunyan');

    var log = new Logger({
      name: 'fash',
      level: 'info'
    });

    var chash = fash.create({
            log: LOG,
            algorithm: 'sha256',
            pnodes: ['A', 'B', 'C', 'D', 'E'],
            vnodes: numberOfVnodes,
            random: true
    });

    var topology;
    chash.on('update', function(ring) {
      //persist the ring
      topology = ring;
    });

    var chash2 = Fash.deserialize({
      log: log,
      algorith: 'sha256',
      topology: toplogy
    });

That's it, chash and chash2 now contain the same ring toplogy.
