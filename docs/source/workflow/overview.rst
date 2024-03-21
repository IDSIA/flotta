==============================
Overview
==============================

The workflow in Ferdelance always starts with a workbench submitting an ``Artifact`` to a scheduler node.
Then, the node will elaborate the artifact and split it in ``tasks`` (or ``jobs``, in this context these two words are used as synonym) and scheduled based on the worker nodes that will execute each task.
The advancement in the completion of the Artifact is strictly controlled by the scheduling node.
During the execution of the tasks, the worker nodes will share ``resources`` between them, as defined in the artifact.
Once all task have been completed, the final resource (it can be a result of a query, or a trained model) is returned to the scheduling node, where it will be possible to download it, if the node configuration allows it.


Artifact
==============================

This is the core unit of the framework.
This object defines the sequence of *steps* that will be deployed and execute in the worker network.
Workbenches submit artifacts to a node in charge to act as a scheduler.

Once submitted, an artifact is converted to a sequence of jobs based on the available worker nodes.
The collection of available nodes define the ``Scheduler Context``.
The scheduler node uses this context and the list of steps defined in the artifact, to define how the jobs will be executed by which worker.
The chosen workers will at this point fetch and execute the tasks assigned.

At any time, the workbench will be able to query the scheduler on the status of the artifact and following its development.
Once all tasks defined by an artifact are completed, and the scheduler node's configuration allows it, it will be possible to download the produced resources.



Step
==============================

The concept of ``Steps`` can be explained with an example.
Imagine to following this simple algorithm::

    data = load_data("awesome_data.csv")
    data = filter(data.x > 0.5)
    res = count(data)

This is just a simple algorithm to load some data from disk, select only the rows where ``x > 5``, and count them.
Now let's make the algorithm distributed, in other words the input data have been split across multiple nodes::

    nodes = [1,2,3]
    res = 0

    for n in nodes:
        data = load_data(f"awesome_data_{n}.csv")
        data = filter(data.x > 0.5)
        res += count(data)

The result we obtain in ``res`` variable is the same as before, but whe have to loop over each node to count how many rows we have in total.

A ``step`` is the code defined inside the loop, with the nuance that it is not executed on the same machine but across multiple machines::

    # step on node 0 (scheduler)
    res_out = 0
    send(1, res_out)

    # step on node 1
    data = load_data("awesome_data_1.csv")
    res_out = step.run(res_in, data)
    send(2, res_out)

    # step on node 2
    data = load_data("awesome_data_2.csv")
    res_out = step.run(res_in, data)
    send(3, res_out)

    # step on node 3
    data = load_data("awesome_data_3.csv")
    res_out = step.run(res_in, data)
    send(0, res_out0) # back to the scheduler

The step on node 0 will perform an initialization.
The step on node 1, 2, and 3 will instead perform the same operations: filtering of the data, count of the remaining rows, and add to the input received from the previous node.
The operations of ``load_data()`` and ``send()`` are defined by the execution process inside a worker node.
This aims to prepare the same *working environment* (mostly a dictionary of variables) on all workers.
xIn this way, each step will always have access to the required data, from local source or received as external resources, and will produce a resource.


Resources
==============================

A ``resource`` is anything that can be exchanged between two nodes and that can be consumed or produced by a task.
Each task will always produce a resource that will be consumed by the next worker node based on the scheduled task.
A task can consume a resource produced by a previous node, but there are tasks that will just work with local data that does not consume extra resources.
