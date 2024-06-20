# %%
from flotta.core.distributions import Collect
from flotta.core.estimators import GroupCountEstimator, MeanEstimator
from flotta.core.model_operations import Aggregation, Train, TrainTest
from flotta.core.models import FederatedRandomForestClassifier, StrategyRandomForestClassifier
from flotta.core.steps import Finalize, Parallel
from flotta.core.transformers import FederatedSplitter, FederatedKBinsDiscretizer
from flotta.schemas.workbench import WorkbenchResource
from flotta.workbench import (
    Context,
    Project,
    Client,
    Artifact,
    ArtifactStatus,
    DataSource,
)

import numpy as np

import json


# %% create the context
ctx = Context("http://localhost:1456")

# %% load a project given a token
project_token = "58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94"

project: Project = ctx.project(project_token)

# %% What is this project?

print(project)

# %% (for DEBUG) ask the context for clients of a project
clients: list[Client] = ctx.clients(project)

for c in clients:
    print(c)

# %% (for DEBUG) ask the context for data source of a project
datasources: list[DataSource] = ctx.datasources(project)

for datasource in datasources:
    print(datasource)  # <--- non aggregated

# %% working with data

ds = project.data  # <--- AggregatedDataSource

print(ds)

# this is like a describe, but per single feature
for feature in ds.features:
    print(feature)

# %% develop a filter query

# prepare transformation query with all features
q = project.extract()

# inspect a feature data type
feature = q["variety"]

print(feature)

# add filter to the extraction query
q = q.add(q["variety"] < 2)

# %% add transformer

q = q.add(
    FederatedKBinsDiscretizer(
        features_in=[q["variety"]],
        features_out=[q["variety_discr"]],
    )
)

# %% statistics 1

gc = GroupCountEstimator(
    query=q,
    by=["variety_discr"],
    features=["variety_discr"],
)

ret = ctx.submit(project, gc.get_steps())

# %% statistics 2

me = MeanEstimator(query=q)

ret = ctx.submit(project, me.get_steps())

# %% prepare the model steps
model = FederatedRandomForestClassifier(
    n_estimators=10,
    strategy=StrategyRandomForestClassifier.MERGE,
)

label = "MedHouseValDiscrete"

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

# %% submit the task to the node, it will be converted to an Artifact

a: Artifact = ctx.submit(project, steps)

print(json.dumps(a.model_dump(), indent=True))  # view execution plan

# %% monitor learning progress
status: ArtifactStatus = ctx.status(a)

print(status)

# %% list produced resources
resources: list[WorkbenchResource] = ctx.list_resources(a)

for r in resources:
    print(r.resource_id, r.creation_time, r.is_ready)

# %% get latest produced resource (the result)

agg_model: FederatedRandomForestClassifier = ctx.get_latest_resource(a)["model"]

# %%

print(agg_model.predict(np.array([[0, 0, 0, 0]])))
print(agg_model.predict(np.array([[1, 1, 1, 1]])))
