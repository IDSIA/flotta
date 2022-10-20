from ferdelance_workbench.context import Context
from ferdelance_workbench.artifacts import (
    Artifact,
    Dataset,
)
from ferdelance_workbench.models import (
    FederatedRandomForestClassifier,
    StrategyRandomForestClassifier,
    ParametersRandomForestClassifier,
)
from dotenv import load_dotenv

import time
import os

load_dotenv()

ctx = Context(f'http://ferdelance.{os.environ.get("DOMAIN")}')

ds_california_1 = None
ds_california_2 = None

for ds_id in ctx.list_datasources():
    ds = ctx.datasource_by_id(ds_id)
    if ds.name == 'california1':
        ds_california_1 = ds
    if ds.name == 'california2':
        ds_california_2 = ds

assert ds_california_1 is not None
assert ds_california_2 is not None

q1 = ds_california_1.all_features()
q2 = ds_california_1.all_features()

d = Dataset(
    test_percentage=0.2,
    val_percentage=0.0,
    label='MedHouseValDiscrete',
)
d.add_query(q1)
d.add_query(q2)

m = FederatedRandomForestClassifier(
    strategy=StrategyRandomForestClassifier.MERGE,
    parameters=ParametersRandomForestClassifier(n_estimators=10)
)

a: Artifact = Artifact(
    dataset=d,
    model=m.build(),
)

a = ctx.submit(a)

print('Artifact id:', a.artifact_id)

while (status := ctx.status(a)).status != 'COMPLETED':
    print(status.status)
    time.sleep(1)
print('done!')

model_path = ctx.get_model(a)

print('model saved to:', model_path)
