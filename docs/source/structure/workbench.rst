==============================
Workbench
==============================


The *workbench* is not a standalone application but a library that need to be imported.
It is used to communicate with a node and submit Artifacts, that encapsulate instructions for the job scheduling and execution.

Installation is straightforward::

  pip install ferdelance[workbench]


Once installed, just create a context object and obtain a project handler with a token.
A project is a collection of data sources.
The token is created by the node network administrator and it is unique for each project.

Following an example of how to use the workbench library to connect to a node. ::


    from ferdelance.core.distributions import Collect
    from ferdelance.core.model_operations import Aggregation, Train, TrainTest
    from ferdelance.core.models import FederatedRandomForestClassifier, StrategyRandomForestClassifier
    from ferdelance.core.steps import Finalize, Parallel
    from ferdelance.core.transformers import FederatedSplitter
    from ferdelance.workbench import Context, Artifact

    server_url = "http://localhost:1456"
    project_token = "58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94"

    # create the context
    ctx = Context(server_url)

    # load a project
    project = ctx.project(project_token)

    # an aggregated view on data
    ds = project.data  

    # print all available features
    for feature in ds.features:
        print(feature)

    # create a query starting from the project's data
    q = ds.extract()
    q = q.add(q["feature"] < 2)

    # create a Federated model
    model = FederatedRandomForestClassifier(
        n_estimators=10,
        strategy=StrategyRandomForestClassifier.MERGE,
    )

    label = "label"

    # describe how to distribute the work and how to train teh model
    steps = [
        Parallel(
            TrainTest(
                query=project.extract().add(
                    FederatedSplitter(
                        random_state=42,
                        test_percentage=0.2,
                        label=label,
                    )
                ),
                trainer=Train(model=model),
                model=model,
            ),
            Collect(),
        ),
        Finalize(
            Aggregation(model=model),
        ),
    ]

    # submit artifact
    artifact: Artifact = ctx.submit(project, steps)

.. Note:
   More examples are available in the `examples <https://github.com/IDSIA/Ferdelance/tree/main/examples>`_ and in the `tests <https://github.com/IDSIA/Ferdelance/tree/main/tests>`_ folders.
