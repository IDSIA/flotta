==============================
Scheduling
==============================

*Scheduling* is the act of convert the steps in an *Artifact* to ``SchedulerJob`` stored in the *database*.
The database contains only metadata regarding the order of the jobs execution.
``Step``\s, associated with a job, are sent in their entirety to the worker node.
This mean that a single *step* is a complete and autonomous piece of data that instruct a node on all the work it needs to do.


SchedulerJob
==============================

A ``SchedulerJob`` defines the *worker* that will execute the task, a series of *locks*, and the *resources* required and produced.

The *locking mechanism* is the most important part of the whole library since it defines the order of jobs of an Artifact needs to be completed.
The list of *locks* in this object is a list of all the jobs that can start once this job is completed successfully.
In case of an error in a single job, the whole Artifact fails.

In the database the *locks* are stored in a dedicated table.
Once a job is completed, this table is updated.
Each time the scheduler is queried for the next available job, the data in the table defines which job to start next.

At higher level, this defines and stores a *Direct Acyclic Graph* (also simply named *DAG*) of the operations.


Binding
==============================

The conversion from sequence of steps to a DAG of operations requires a ``SchedulerContext``.
This is a description of the available nodes that can execute the tasks.
A worker node is valid when it has *data* that are part of the Artifact's project.

A particular worker of the *SchedulerContext* is the ``Initiator``.
This worker, in the vast majority of the cases, is the scheduler node itself.
An *Initiator* does not requires to have local data to be part of the context, since it is used to perform initialization (setup), aggregation, and completion (teardown) operations.

The context keep also track of the numeric ids of the jobs.
These ids will be used to keep track of *locks* between jobs during the binding procedure.
Once completed, they are converted to UUIDs before storing them in teh database.

The *binding procedure* creates in fact the *locks* between the jobs.
It starts from iterating pairwise through the steps, converting each step in a *SchedulerJob*, and considering the distribution operations associated to each step to build the *locks*.
