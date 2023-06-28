# %%
from ferdelance.workbench.context import Context
from ferdelance.schemas.queries import Query
from ferdelance.schemas.datasources import DataSource
from ferdelance.schemas.project import Project
from ferdelance.schemas.transformers import (
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

p: Project = ctx.project("")

# %% ask the context for available metadata
data_sources: list[DataSource] = ctx.datasources(p)

ds1: DataSource = data_sources[0]
ds2: DataSource = data_sources[1]

# %% develop a filter query
q: Query = ds1.extract()

all_features = q.features()

f1, f2, f3, f4, f5, f6, f7, f8, f9 = all_features

mms = FederatedMinMaxScaler(all_features, all_features)
im = FederatedSimpleImputer([f1, f2], [f1, f2])
cl = FederatedClamp(f1, f1)
bi = FederatedBinarizer(f2, f2)

ohe1 = FederatedOneHotEncoder(f3)
ohe2 = FederatedOneHotEncoder(f4)
rm = FederatedDrop(ohe1.features_out[2])

dsc = FederatedKBinsDiscretizer(f5, f5)
le = FederatedLabelBinarizer(f9, f9)

pipe = FederatedPipeline(
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

q.add(pipe)
