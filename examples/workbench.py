# %%
from joblib import Parallel
from ferdelance.core.distributions import Collect
from ferdelance.core.estimators import GroupCountEstimator, MeanEstimator
from ferdelance.core.models import FederatedRandomForestClassifier, StrategyRandomForestClassifier
from ferdelance.core.model_operations import Aggregation, Train, TrainTest
from ferdelance.core.steps import Finalize
from ferdelance.core.transformers import FederatedSplitter, FederatedKBinsDiscretizer
from ferdelance.workbench import (
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

# nice to have:
# with Context(...) as ctx:
#     ...

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
m = FederatedRandomForestClassifier(
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
            trainer=Train(model=m),
            model=m,
        ),
        Collect(),
    ),
    Finalize(
        Aggregation(model=m),
    ),
]

# %% submit the task to the node, it will be converted to an Artifact

a: Artifact = ctx.submit(project, steps)

print(json.dumps(a.dict(), indent=True))  # view execution plan

# %% monitor learning progress
status: ArtifactStatus = ctx.status(a)

print(status)

# %% download trained model:
m.load(ctx.get_result(a))

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
