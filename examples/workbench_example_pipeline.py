# %%
from ferdelance.workbench.context import Context
from ferdelance.shared.artifacts import (
    Dataset,
    Query,
    DataSource,
)
from ferdelance.shared.transformers import (
    FederatedPipeline,
    FederatedMinMaxScaler,
    FederatedSimpleImputer,
    FederatedDrop,
    FederatedKBinsDiscretizer,
    FederatedLabelBinarizer,
    FederatedBinarizer,
    FederatedOneHotEncoder,
    FederatedClamp,
)

# %% create the context
ctx = Context("http://ferdelance.artemis.idsia.ch")

# %% ask the context for available metadata
data_sources_id: list[str] = ctx.list_datasources()

ds1: DataSource = ctx.get_datasource_by_id(data_sources_id[0])
ds2: DataSource = ctx.get_datasource_by_id(data_sources_id[1])

# %% develop a filter query
q: Query = ds1.all_features()

f1, f2, f3, f4, f5, f6, f7, f8, f9 = q.features

mms = FederatedMinMaxScaler(q.features, q.features)
im = FederatedSimpleImputer([f1, f2], [f1, f2])
cl = FederatedClamp(f1, f1)
bi = FederatedBinarizer(f2, f2)

ohe1 = FederatedOneHotEncoder([f3])
ohe2 = FederatedOneHotEncoder([f4])
rm = FederatedDrop(ohe.out[2])

dsc = FederatedKBinsDiscretizer(f5, f5)
le = FederatedLabelBinarizer(f9, f9)

p = FederatedPipeline(
    [
        mms,
        im,
        cl,
        bi,
        ohe1,
        ohe2,
        rm,
        dsc,
        le,
    ]
)

# %% create dataset
d = Dataset(
    test_percentage=0.2,
    val_percentage=0.1,
    label="variety",
)
d.add_query(q)
