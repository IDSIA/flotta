==============================
Execution mode
==============================

A node in flotta can be executed in three different flavours, based on the needs of the data provider: ``node``, ``client``, and ``standalone`` mode.


Node mode
==============================

This is the standard execution mode of a node.
In this mode, the API are accessible from the extern.
At least one node in the network need to be executed with this mode to allow other nodes to join.


Client mode
==============================

This is a special mode suited for all the nodes that should not be reachable from the external world.
In this mode, after joining a network through a node that will be marked as ``scheduler``, the client node will launch a polling thread that will interact with the join node at regular intervals.
At each interaction, the node will requires an updates on the task to execute.
When the scheduler node notifies a new job to be executed, it is the client node to contact the scheduler and fetch all the required data.

In this mode the APIs are accessibly only locally for maintenance purposes.
The other nodes will not be able to interact directly with this node.

Although this guarantees an higher level of security, this mode has the drawback that some algorithms cannot be executed or need to be executed in a different way, such as through the use of the scheduler node as proxy node.


Standalone mode
==============================

One of the simplest way to execute and test changes to the framework is through the so called ``standalone mode``.
In this mode, the framework is executed as a standalone application.
The hardcoded configuration guarantees that a scheduling node capable of scheduling jobs for itself with is deployed.
To run in standalone mode there is no need to pass, although it is possible, a configuration parameter::

  python -m flotta.standalone

This mode is not suitable for production environment and should be used only for learning of testing purposes.
