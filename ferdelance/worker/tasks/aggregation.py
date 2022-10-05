from ferdelance_shared.schemas import Artifact

from ..models import AggregatorRandomForestClassifier

from ..celery import worker

import logging
import pickle
import requests

LOGGER = logging.getLogger(__name__)


@worker.task(
    ignore_result=False,
    bind=True,
)
def aggregation(self, token: str, artifact_id: str, model_ids: list[str]) -> str:

    task_id = str(self.request.id)

    LOGGER.info(f'beginning aggregation task={task_id} for artifact_id={artifact_id}')

    content = requests.get(
        f'http://server/worker/artifact/{artifact_id}',
        headers={'Authorization': f'Bearer {token}'},
    )
    artifact = Artifact(**content.json())

    models = []
    for model_id in model_ids:
        LOGGER.info(f'requesting {model_id}')
        res = requests.get(f'http://server/worker/model/{model_id}')
        model = pickle.loads(res.content)
        models.append(model)

    LOGGER.info(f'fetched {len(models)} model(s)')

    model_name = artifact.model.name

    if model_name == 'FederatedRandomForestClassifier':
        agg = AggregatorRandomForestClassifier()
        aggregated_model = agg.aggregate(artifact.model.strategy, models)

    else:
        raise ValueError(f'Unsupported model: {model_name}')

    requests.post(
        'http://server/worker/model/aggregated/{artifact_id}',
        headers={'Authorization': f'Bearer {token}'},
        files={'aggregated_model': pickle.dumps(aggregated_model)}
    )
