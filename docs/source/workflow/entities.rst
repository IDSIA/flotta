==============================
Core objects
==============================

Core objects can be instantiated from class defined in the ``ferdelance.core`` submodule.
In this page we explain the thought flow from a code point of view.

Entities
==============================

Anything that can be exchanged through the API of a node and compose an Artifact is an *entity*.
``Entity`` is the main class that defines the hierarchy of objects that can be used in an Artifact.
This class defines a ``class_registry`` that is used by the API to convert JSON objects to Python objects, and vice versa, using `Pydantic <https://docs.pydantic.dev/>`_'s un/marshalling mechanism.

New classes must extend this class in order to work.


Artifacts
==============================

``Artifact`` class is an entity with a Sequence of *steps*.
The Artifact defines how ``SchedulerJob``s objects are created given a ``SchedulerContext``.
The creation is simple: for each pair of consecutive steps, transform the jobs and bind them.
Binding means writing a direct acyclic graph defining the Artifact jobs using a lock mechanism.
These locks defines which job waits for other jobs to complete successfully before it can be executed.


Steps
==============================

A ``Step`` is a fundamental abstract class that, in fact, defines an Artifact.
Steps have three important methods:

- ``jobs()`` convert the step in a sequence of ``SchedulerJob``\s (a single step can generate many jobs).
- ``bind()`` executed by the scheduler, it is used to create a connection between the step itself, the ``SchedulerJob``\s before and  the ``SchedulerJob`` after the step execution.
- ``step()`` executed by the workers, runs the code based on the available local data.

A main implementation of these methods is offered by following classes:

- ``BaseStep``, as the name suggests, is a basic implementation of a generic Step and it is composed of an ``Operation`` (the work to do) object and a ``Distribution`` object (how to share the resources between nodes).
- ``Iterate``, instead, is a *"meta step"* that wraps a sequence of steps allowing the repetition of them, up to a limited number.

The ``Iterate`` class is needed to allow the scheduler to implement a particular kind of bind that allows the iteration.
In fact, this class does not implement a real ``bind()`` method, instead wraps it in a continued for loop.

The ``BaseStep`` class is extended by the following classes, allowing the execution of basic procedures and implementing at the same time a distribution:

- ``Initialize`` schedules a setup operation on the initiator node.
- ``Finalize`` schedules a teardown operation on the initiator node.
- ``Parallel`` schedules an operations in parallel across all available worker nodes.
- ``Sequential`` schedules an operation on each node where the execution of the next job requires the completion of all the previous jobs.
- ``RoundRobin`` is a particular step where the resources are exchanged following a circle. Let's assume we have ``N`` workers. Each worker receive a task that updates a resource using local data. Once updated, this resource is sent to the next node that will update it with its local data. This update-and-exchange is iterated until all nodes have contributed to the resource update. In fact the updates happens in parallel: received the initial resource, all nodes update it, and then send it to the next worker, updating. After ``N`` updates, all nodes have the resources with the contributions of all workers.


Distributors
==============================

Distribution objects defines how the resources are distributed between nodes.
