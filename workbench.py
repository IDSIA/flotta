# %%
from ferdelance.workbench.context import Context
from ferdelance_shared.artifacts import (
    Artifact,
    ArtifactStatus,
    Dataset,
    Query,
    DataSource,
)
from ferdelance_shared.models import (
    FederatedRandomForestClassifier,
    StrategyRandomForestClassifier,
    ParametersRandomForestClassifier,
)

import numpy as np

import json

# %% create the context
ctx = Context('http://ferdelance.artemis.idsia.ch')

# %% ask the context for available client
for c in ctx.list_clients():
    dc = ctx.detail_client(c)
    print(dc.client_id)
    print('- client version:', dc.version)

# %% ask the context for available metadata
data_sources_id: list[str] = ctx.list_datasources()

ds: DataSource | None = None

for ds_id in data_sources_id:
    ds = ctx.datasource_by_id(ds_id)

    if ds.name == 'iris':
        break

assert ds is not None

print(ds.info())

# %% develop a filter query
q: Query = ds.all_features()

for feature in q.features:
    print(ds[feature].info())
    print()


# # add filters
q = q[ds['variety'] < 2]

# %% create dataset
d = Dataset(
    test_percentage=0.2,
    val_percentage=0.1,
    label='variety',
)
d.add_query(q)

# %% develop a model

m = FederatedRandomForestClassifier(
    strategy=StrategyRandomForestClassifier.MERGE,
    parameters=ParametersRandomForestClassifier(n_estimators=10)
)

# %% create an artifact and deploy query, model, and strategy
a: Artifact = Artifact(
    dataset=d,
    model=m.build(),
)

print(json.dumps(a.dict(), indent=True))

# %% submit artifact to the server
a = ctx.submit(a)

assert a.artifact_id is not None

# %% monitor learning progress
status: ArtifactStatus = ctx.status(a)

print(status)

# %% download submitted artifact
art = ctx.get_artifact(a.artifact_id)

print(art)

# %% download trained model:
m.load(ctx.get_model(a))

# %%

print(m.predict(np.array([[0, 0, 0, 0]])))
print(m.predict(np.array([[1, 1, 1, 1]])))

# %%
