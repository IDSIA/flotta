# %%
from ferdelance.workbench import (
    Context,
    Project,
    Client,
    Artifact,
    ArtifactStatus,
    DataSource,
)
from ferdelance.schemas.models import (
    FederatedRandomForestClassifier,
    ParametersRandomForestClassifier,
    StrategyRandomForestClassifier,
)
from ferdelance.schemas.plans import TrainTestSplit
from ferdelance.schemas.transformers import FederatedKBinsDiscretizer

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

# list of AggregatedFeature:
# distributions, not discrete values
# min, max, std, mean, %missing
# total amount of records
# in tabular (dataframe) format

# this is like a describe, but per single feature
for feature in ds.features:
    print(feature)

# %% develop a filter query

# prepare transformation query with all features
q = ds.extract()

# inspect a feature data type
feature = q["variety"]

print(feature)

# add filter
q = q.add(q["variety"] < 2)

# %% add transformer

q = q.add(
    FederatedKBinsDiscretizer(
        q["variety"],
        "variety_discr",
    )
)

# %% statistics 1

s1 = q.groupby(q["variety"])
s1 = s1.count()

# TODO: these statistics requires a new scheduler and a cache system on the client

# this is a special kind of submit where the request hangs until the results is available
ret = ctx.execute(project, s1)

# output is something like:
# submit: .........done!

# %% statistics 2

s2 = q.mean(q["variety"])

ret = ctx.execute(project, s1)

# %% create an execution plan

q = q.add_plan(
    TrainTestSplit(
        label="variety",
        test_percentage=0.5,
    )
)

# %% develop a model

m = FederatedRandomForestClassifier(
    strategy=StrategyRandomForestClassifier.MERGE,
    parameters=ParametersRandomForestClassifier(n_estimators=10),
)

q = q.add_model(m)

# %% submit the task to the server, it will be converted to an Artifact

a: Artifact = ctx.submit(project, q)

print(json.dumps(a.dict(), indent=True))  # view execution plan

# %% monitor learning progress
status: ArtifactStatus = ctx.status(a)

print(status)

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
