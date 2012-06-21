# fash: consistent hashing library for node.js

This module provides a consistent hashing library. The 'one more thing' feature this provides is the ability to **deterministically generate the same hash ring topology across a set of distributed hosts**. Fash also handles collisions of nodes on the ring and ensures no two nodes will share the same spot on the ring.

# Example

## Boostrapping a New Hash Ring

    var fash = require('fash');
    var Logger = require('bunyan');

    var log = new Logger({
      name: 'fash',
      level: 'info'
    });

    var chash = fash.createHash({
      log: log, // optional [bunyan](https://github.com/trentm/node-bunyan) log object.
      algorithm: 'sha256', // Can be any algorithm supported by openssl.
      nodes: ['A', 'B', 'C', 'D', 'E'], // The set of nodes to insert onto the ring.
      numberOfReplicas: 1 // The number of replica nodes for each physical node to insert on the ring.
    });

    var node = chash.getNode('someKeyToHash');

If the config used to bootstrap fash is the same across all clients, then the ring toplogy will be the same as well. `numberOfReplicas` is used to determine the number of replicas each node has within the ring, these allow the ability to map one node to multiple points on the ring.

## Adding More Nodes to the Ring
You can add additional nodes to the ring after fash has been initialized.

    // add the physical node 'someNode' to the ring.
    chash.addNode('someNode');

Additionally, you can specify the number of replicas for each additional node you add. The default `numberOfReplicas` is used if it's not specified.  As long as you ensure the order of adds is consistent across all clients, the ring toplogy should be consistent as well.

## Removing Nodes from the Ring
You can remove nodes from the ring like so.

    chash.removeNode('someNode');

Note the node and any replicas of the node are removed. Again, as long as the order of removes is consistent across all clients, the ring toplogy will be consistent as well.

## Persisting Ring Toplogy

At any time, the ring toplogy can be accessed by:

    chash.ring;

The ring toplogy is an array of

    {
      hashSpace, // the hashspace in a hex string that this node occupies.
      node, // the phsyical node name.
      replica // the node replica name.
    }

The array is sorted by hashSpace. Additionally, anytime removeNode or addNode is invoked, fash emits an `update` event which contains an updated version of the ring array. Fash can be instantiated given a topology object instead of a list of nodes.

    var topology;
    chash.on('update', function(ring) {
      //persist the ring
      topology = ring;
    });

    var chash2 = new Fash({
      log: log,
      algorith: 'sha256',
      nodes: toplogy,
      numberOfReplicas: 1
    });

That's it, chash and chash2 now contain the same ring toplogy.
