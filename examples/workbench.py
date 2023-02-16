# %%
from ferdelance.schemas.models import (
    FederatedRandomForestClassifier,
    ParametersRandomForestClassifier,
    StrategyRandomForestClassifier,
)
from ferdelance.workbench.context import Context
from ferdelance.workbench.interface import (
    Project,
    Client,
    Artifact,
    ArtifactStatus,
    DataSource,
    AggregatedDataSource,
    AggregatedFeature,
    ExecutionPlan,
)

import numpy as np

import json


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

ds = project.data  # <--- AggregatedDataSource

print(ds.describe())

# list of AggregatedFeature:
# distributions, not discrete values
# min, max, std, mean, %missing
# total amount of records
# in tabular (dataframe) format

# this is like a describe, but per single feature
for feature in ds.features:
    print(feature)

# %% develop a filter query

from ferdelance.schemas.transformers import FederatedKBinsDiscretizer

q = ds.extract()

feature = q["variety"]
filter = q["variety"] < 2
transformer = FederatedKBinsDiscretizer(q["variety"], "variety_discr")

q.add(q["variety"] < 2)
q.add(transformer)

# add filters

# datasource (ds) is composed by a series of stages
# each stage keep track of the current available features
# a stage is updated each time a new operation is added on the features
#
# stage 0:  f1  f2  f3  f4
#  ---> drop f1
# stage 1:      f2  f3  f4
#  ---> discretize f5 in 2 bins
# stage 2:      f2  f3  f41  f42
#  ---> filter f2 > x
# stage 3:      f2x f3x f41x f42x
#
#  -> keep track of available features and dtypes at current stage
#
# this will create a single pipelines of opearations:
#
#  [
#        select(f1, f2, f3, f4),
#        drop(f1),
#        discretize(f4, 2),
#        filter(f2 > x),
#  ]
#
# each client will receive the whole pipeline adapted for its features
#  -> check that some operations (such as filter) need to have the feature available on all clients
#  -> add the number of clients that have have the feature to all AggregatedFeature
#  -> raise error workbench-side with certain operations

# other implementations of filters, transformers, ...

# %% statistics
"""
TODO: these statistics requires a new scheduler and a cache system on the client
stats = Statistics(
    data=ds,
)
ret = ctx.statistics(stats)  # partial submit
"""

# %% develop a model

m = FederatedRandomForestClassifier(
    strategy=StrategyRandomForestClassifier.MERGE,
    parameters=ParametersRandomForestClassifier(n_estimators=10),
)

# %% create an artifact and deploy query, model, and strategy
a: Artifact = Artifact(
    label="variety",
    model=m.build(),
    transform=q,
    load=ExecutionPlan(
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


# %%

# AggregatedDataSource(
#     project_id="123-456-789",
#     project_name="Housing",
#     n_records=69,
#     n_features=420,
#     features=[
#         AggregatedFeature(
#             "Price",
#             float,
#             min=Distrib(
#                 min=1000,
#                 max=2000,
#                 mean=1600,
#                 stdd=3.5
#             ),
#             max=Distrib(
#                 ...
#             ),
#             ...
#         )
#     ]
# )
