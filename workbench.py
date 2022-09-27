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

for ds in data_sources_id:
    dds: DataSource = ctx.detail_datasource(ds)
    print(f'{dds.datasource_id}')
    for df in dds.features:
        print(f'- {df.dtype:8} {df.name:4} {df.v_mean}')

# %% develop a filter query
ds_id = data_sources_id[0]

dds = ctx.detail_datasource(ds_id)

q = Query(
    datasources_id=dds.datasource_id,
    features=[QueryFeature(feature_id=f.feature_id, datasource_id=f.datasource_id) for f in dds.features]
)

# %% develop a model
m = Model(name='example_model', model=None)

# %% develop an aggregation strategy
s = Strategy(strategy='nothing')

# %% create an artifact and deploy query, model, and strategy to the server
a: Artifact = Artifact(
    queries=[q],
    model=m,
    strategy=s,
)

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
