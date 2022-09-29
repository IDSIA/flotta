# %%
from ferdelance_workbench.context import Context
from ferdelance_workbench.artifacts import Artifact, ArtifactStatus, Query, QueryFeature, Model, Strategy, DataSource

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

# %% develop a model
m = Model(name='example_model', model=None)

# %% develop an aggregation strategy
s = Strategy(strategy='nothing')

# %% create an artifact and deploy query, model, and strategy
a: Artifact = Artifact(
    queries=[q],
    model=m,
    strategy=s,
)

# %% submit artifact to the server
a, status = ctx.submit(a, True)

print(status)

# %% monitor learning progress
a: ArtifactStatus = ctx.status(a)

print(a)

# %% download submitted artifact
art = ctx.get_artifact(a.artifact_id)

print(art)

# %% download trained model:
ctx.get_model_to_disk(a, 'model.bin')

# %%
