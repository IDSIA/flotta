# %%
import json

import numpy as np

from ferdelance.shared.artifacts import Artifact, ArtifactStatus, Dataset, DataSource, Query
from ferdelance.shared.models import (
    FederatedRandomForestClassifier,
    ParametersRandomForestClassifier,
    StrategyRandomForestClassifier,
)
from ferdelance.workbench import Context, Project

# %% create the context
ctx = Context("http://localhost:1456")

# nice to have:
# with Context(...) as ctx:
#     ...

# %% load a project given a token
project_token = "58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94"

project: Project = ctx.load(project_token)  # if None check for environment variable

# %% What is this project?

project.describe()

# class Project:
#    data: AggregatedDataSource
#    project_id: str
#    name: str
#    descr: str
#    n_datasources: int
#    n_clients: int


# %% (for DEBUG) ask the context for clients of a project
clients: list[Client] = ctx.clients(project)

for c in clients:
    print(c)

# class Client:
#   client_id: str
#   version: str
#   name: str # <--- TODO: to implement in client

# %% (for DEBUG) ask the context for data source of a project
datasources: list[DataSource] = ctx.datasources(project)

for datasource in datasources:
    print(datasource)  # <--- non aggregated


# %% working with data

ds: AggregatedDataSource = project.data  # <--- aggregated data source

print(ds.describe())

# list of AggregatedFeature:
# distributions, not discrete values
# min, max, std, mean, %missing
# total amount of records
# in tabular (dataframe) format

# this is like a describe, but per single feature
for feature in ds.features:
    print(ds[feature])

# %% develop a filter query

# # add filters
ds = ds[ds["variety"] < 2]  # returns a datasource updated

# other implementations of filters, transformers, ...

# %% statistics
stats = Statistics(
    data=ds,
)
ret = ctx.statistics(stats)  # partial submit

# %% develop a model

m = FederatedRandomForestClassifier(
    strategy=StrategyRandomForestClassifier.MERGE,
    parameters=ParametersRandomForestClassifier(n_estimators=10),
)

# %% create an artifact and deploy query, model, and strategy
a: Artifact = Artifact(
    data=ds,
    label="variety",
    model=m.build(),
    how=ExecutionPlan(
        test_percentage=0.2,
        val_percentage=0.1,
        # metrics to track...
        # TrainingStrategy + EvalStrategy:
        #    - cross-validation (fold, fold-size, ...)
        #    - train-test
        #    - train-test-val
        #    - leave-one-out
        #    - iterative training
    ),
)

print(json.dumps(a.dict(), indent=True))  # view execution plan

# %% submit artifact to the server
a = ctx.submit(a)  # or execute

assert a.artifact_id is not None

# %% monitor learning progress
status: ArtifactStatus = ctx.status(a)

print(status)

# %% evaluation


# %% download trained model:
m.load(ctx.get_model(a))

# %%

print(m.predict(np.array([[0, 0, 0, 0]])))
print(m.predict(np.array([[1, 1, 1, 1]])))
