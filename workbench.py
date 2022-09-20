# %%
from ferdelance_workbench.context import Context
from ferdelance_workbench.artifacts import Artifact, Query, Model, Strategy

# %% create the context
ctx = Context('http://ferdelance.artemis.idsia.ch')

# %% ask the context for available client
for c in ctx.list_clients():
    dc = ctx.detail_client(c)
    print(dc.client_id)
    print(dc.created_at)
    print(dc.version)

# %% ask the context for available metadata
data_sources_id: list[int] = ctx.list_datasources()

for ds in data_sources_id:
    dds = ctx.detail_datasource(ds)
    print(f'{dds.datasource.type:5} {dds.datasource.name}')
    for df in dds.features:
        print(f'{df.feature_id:3} {df.dtype:8} {df.name}')

# %% develop a filter query
ds_id = data_sources_id[0]

dds = ctx.detail_datasource(ds_id)

q = Query()
q.add_datasource(dds)

# %% develop a model
m = Model('example_model', None)

# %% develop an aggregation strategy
s = Strategy('nothing')

# %% create an artifact and deploy query, model, and strategy to the server
a: Artifact = ctx.submit(q, m, s)

print(a)

# %% monitor learning progress
a = ctx.status(a)

print(a)

# %% download submitted artifact
art = ctx.get_artifact(a.artifact_id)

print(art)

# %% download trained model:
ctx.get_model(a, 'model.bin')

# %%
