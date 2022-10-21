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

if __name__ == '__main__':

    ctx = Context(f'http://ferdelance.{os.environ.get("DOMAIN")}')

    ds_california_1 = ctx.datasources_by_name('california1')[0]
    ds_california_2 = ctx.datasources_by_name('california2')[0]

    q1 = ds_california_1.all_features()
    q2 = ds_california_2.all_features()

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

    last_state = ''
    while (status := ctx.status(a)).status != 'COMPLETED':
        if status.status == last_state:
            print('.', end='', flush=True)
        else:
            last_state = status.status
            print(last_state, end='', flush=True)
        time.sleep(0.5)
    print('done!')

    model_path = ctx.get_model(a)

    print('model saved to:          ', model_path)

    partial_model_path_1 = ctx.get_partial_model(a, ds_california_1.client_id)

    print('partial model 1 saved to:', partial_model_path_1)

    partial_model_path_2 = ctx.get_partial_model(a, ds_california_2.client_id)

    print('partial model 2 saved to:', partial_model_path_2)
