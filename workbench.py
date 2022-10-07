# %%
from ferdelance_workbench.context import Context
from ferdelance_workbench.artifacts import Artifact, ArtifactStatus, Dataset, Query, DataSource
from ferdelance_workbench.models import FederatedRandomForestClassifier, StrategyRandomForestClassifier

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

ds: DataSource = None

for ds in data_sources_id:
    dds: DataSource = ctx.detail_datasource(ds)

    if dds.name == 'earthquakes':
        ds = dds

print(ds.info())

# %% feature analysis

for k in ds.features_dict().keys():
    print(k)

# %% develop a filter query
q: Query = ds.all_features()
# remove features
q -= ds['ID']
q -= ds['Latitude']
q -= ds['Longitude']

# add filters
q = q[ds['Depth'] > 5.0]
q += ds['Magnitude'] <= 3.0

# %% create dataset
d = Dataset(
    test_percentage=0.2,
    val_percentage=0.1,
    label='Magnitude'
)
d.add_query(q)

# %% develop a model

m = FederatedRandomForestClassifier(
    strategy=StrategyRandomForestClassifier.MERGE,
    n_estimators=10,
)

# %% create an artifact and deploy query, model, and strategy
a: Artifact = Artifact(
    dataset=d,
    model=m,
)

print(json.dumps(a.dict(), indent=True))

# %% submit artifact to the server
status = ctx.submit(a)

print(status)

# %% monitor learning progress
a: ArtifactStatus = ctx.status(a)

print(a)

# %% download submitted artifact
art = ctx.get_artifact(a.artifact_id)

print(art)

# %% download trained model:
m = ctx.get_model(a)

# %%
