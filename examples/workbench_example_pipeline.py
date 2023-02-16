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

p: Project = ctx.load("")

# %% ask the context for available metadata
data_sources: list[DataSource] = ctx.datasources(p)

ds1: DataSource = data_sources[0]
ds2: DataSource = data_sources[1]

# %% develop a filter query
q: Query = ds1.extract()

f1, f2, f3, f4, f5, f6, f7, f8, f9 = q.features()

mms = FederatedMinMaxScaler(q.features(), q.features())
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

# %% create dataset
d = Dataset(
    test_percentage=0.2,
    val_percentage=0.1,
    label="variety",
)
