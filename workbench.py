# %%
from ferdelance_workbench.context import Context
from ferdelance_workbench.artifacts import Artifact, ArtifactStatus, Dataset, Query, DataSource
from ferdelance_workbench.models import FederatedRandomForestClassifier, StrategyRandomForestClassifier, ParametersRandomForestClassifier

import json

# %% create the context
ctx = Context('http://ferdelance.artemis.idsia.ch')

# %% ask the context for available client
for c in ctx.list_clients():
    dc = ctx.detail_client(c)
    print(dc.client_id)
    print('- creation time: ', dc.created_at)
    print('- client version:', dc.version)

# %% ask the context for available metadata
data_sources_id: list[str] = ctx.list_datasources()

ds: DataSource | None = None

for ds_id in data_sources_id:
    ds = ctx.detail_datasource(ds_id)

    if ds.name == 'earthquakes':
        break

assert ds is not None

print(ds.info())

# %% feature analysis

ds = ctx.detail_datasource(data_sources_id[-1])

# %% develop a filter query
q: Query = ds.all_features()

# remove features
for qf in q.features:
    f = ds[qf]
    if f.dtype != 'float64':
        q -= f

    if f.v_miss is None or f.v_miss > 0:
        q -= f

for feature in q.features:
    print(ds[feature].info())
    print()


# # add filters
q = q[ds['Depth'] > 30.0]
q += ds['Magnitude'] >= 6.0

# %% create dataset
d = Dataset(
    test_percentage=0.2,
    val_percentage=0.1,
    label='Magnitude',
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

print(a)

# %% download submitted artifact
art = ctx.get_artifact(a.artifact_id)

print(art)

# %% download trained model:
m = ctx.get_model(a)

# %%
