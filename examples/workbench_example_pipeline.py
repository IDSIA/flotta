# %%
from ferdelance.core.queries import Query
from ferdelance.core.transformers import (
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
from ferdelance.schemas.datasources import DataSource
from ferdelance.schemas.project import Project
from ferdelance.workbench.context import Context

# %% create the context
ctx = Context("http://localhost:1456")

p: Project = ctx.project("")

# %% ask the context for available metadata
data_sources: list[DataSource] = ctx.datasources(p)

ds1: DataSource = data_sources[0]
ds2: DataSource = data_sources[1]

# %% develop a filter query
q: Query = ds1.extract()

all_features = q.features()

f1, f2, f3, f4, f5, f6, f7, f8, f9 = all_features

mms = FederatedMinMaxScaler(features_in=all_features, features_out=all_features)
im = FederatedSimpleImputer(features_in=[f1, f2], features_out=[f1, f2])
cl = FederatedClamp(features_in=[f1], features_out=[f1])
bi = FederatedBinarizer(features_in=[f2], features_out=[f2])

ohe1 = FederatedOneHotEncoder(features_in=[f3])
ohe2 = FederatedOneHotEncoder(features_in=[f4])
rm = FederatedDrop(features_in=[ohe1.features_out[2]])

dsc = FederatedKBinsDiscretizer(features_in=[f5], features_out=[f5])
le = FederatedLabelBinarizer(features_in=[f9], features_out=[f9])

pipe = FederatedPipeline(
    stages=[
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
