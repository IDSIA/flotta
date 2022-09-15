# %%
from ferdelance_workbench.context import Context

# %%

ctx = Context('http://spearhead.artemis.idsia.ch')

# %%
for c in ctx.list_clients():
    print(c)

# %%

for ds in ctx.list_datasources():
    print(ds)
    print(ctx.detail_datasource(ds['datasource_id']))
    print()

# %%
