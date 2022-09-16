# %%
from ferdelance_workbench.context import Context
import json

# %%

ctx = Context('http://ferdelance.artemis.idsia.ch')


def pretty_print(content: dict):
    print(json.dumps(content, sort_keys=True, indent=4))


# %%
for c in ctx.list_clients():
    pretty_print(ctx.detail_client(c))

# %%

for ds in ctx.list_datasources():
    pretty_print(ctx.detail_datasource(ds))
    print()

# %%
