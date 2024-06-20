==============================
Core objects
==============================

Core objects can be instantiated from class defined in the ``flotta.core`` submodule.
In this page we explain the thought flow from a code point of view.

Entities
==============================

Anything that can be exchanged through the API of a node and compose an Artifact is an *entity*.
``Entity`` is the main class that defines the hierarchy of objects that can be used in an Artifact.
This class defines a ``class_registry`` that is used by the API to convert JSON objects to Python objects, and vice versa, using `Pydantic <https://docs.pydantic.dev/>`_'s un/marshalling mechanism.

This is a design choice to make possible the transferring of "code" between a workbench and the worker nodes.
The library will provide a series of *blocks of code* ready to be used.
The composition of such elements creates an ``Artifact``.

New classes must extend the Entity class in order to work.


Artifacts
==============================

``Artifact`` class is an entity with a Sequence of *steps*.
The Artifact defines how ``SchedulerJob``\s objects are created given a ``SchedulerContext``.
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
The distribution of resources and the *locking* mechanism are tight together.
When a job is *locked*, in fact it is waiting for all required resources to be available.

In general, a task can be in charge of both pull the required resources from other nodes or push them to the next node.
In both cases, when a step is executed it expects to have all resources available locally, independently if they need to be produced (resources) or not (local data sources).

The main Distribution class is the ``Arrange`` base algorithm that allows the nodes to collect resources from, and distribute them to multiple nodes. The variant ``Distribute`` allows the distribution from one node to multiple nodes, while ``Collect`` awaits for the availability of multiple resources sent to one node. In fact, these two variants are particular cases of the Arrange class.


Operation and Worker's task
==============================

The task that will be executed by a worker node is essentially defined through the ``Operation`` class.

The first, is that to manipulate and select local data, a ``Query`` need to be defined and performed.
Queries are much like `Panda <https://pandas.pydata.org/>`_'s DataFrame operations, given also the fact that this is the underling library used to manipulate local data.

The second and third concept are interchangeable and cannot coexist at the same time.
Each task will produce either a trained ``Model`` or an ``Estimator``.
``Model``\s starts from the data selected through the Query and are assembled through Machine Learning training algorithms.
Once a Model has been built it became a resource that can be exchanged.

An ``Estimator``, on the other hand, is much more similar to a results of a database's query.
Exploratory analysis, statistical data, and aggregated information can be collected from the distributed data trough the use of a dedicated Estimator.


Queries and Data Transformers
------------------------------

Queries to select, manipulate, and create new features can be created through the composition of ``QueryFilters`` and the use of ``Transformers``.

Each column available in the local data is a ``QueryFeature``.
From each of them it is possible to concatenate ``QueryFilters``, to reduce the number of data available, and create complex ``FederatedPipeline`` of Transformers to manipulate the data.

These Transformers are wrappers of existing `Scikit Learn <https://scikit-learn.org/stable/>`_ pre-processing algorithms.


Models and Model Operations
------------------------------

``Model`` is an abstract class that offers all the methods to `load()`, `save()`, `train()`, `aggregate()`, and use a Machine Learning Model.

Given a distributed context like in a Federated Learning environment, the *aggregation process* of a Machine Learning model is something that is defined by the model itself.

The `eval()` method implements a generic and powerful analysis of the produced model.

As one can see, the `train()`, `predict()`, and `classify()` methods requires input data in form of `X` and `Y` parameters.
These data can be created using ``ModelOperations``.
Through the composition of operations such as ``Train``, ``TrainTest``, or ``LocalCrossValidation``, the worker node will be instructed to perform machine learning task to prepare the data and train the models.


Estimators 
-----------------------------------------

The ``Estimator`` abstract class offers a way to manipulate the output of a ``Query`` and perform statistical analysis such as:

- ``CountEstimator`` and ``GroupCountEstimator`` are used to count the number of records available across the nodes.
- ``MeanEstimator`` and ``GroupMeanEstimator`` are used to get the mean of the variable across the nodes.

Count and mean operations uses *noise* and requires a Sequential task order to increase privacy and hide the number of records on each node with data.

The objective of these blocks is to offer to the researchers a way to inspect data in a secure and privacy friendly way, keeping at the same time a familiar way of visualize and interact with the distributed data.
