from ferdelance_shared.schemas import Artifact

from ..models import AggregatorRandomForestClassifier

from ..celery import worker

import logging
import os
import pickle
import requests

LOGGER = logging.getLogger(__name__)


@worker.task(
    ignore_result=False,
    bind=True,
)
def aggregation(self, token: str, artifact_id: str, model_ids: list[str]) -> str:
    try:
        task_id: str = str(self.request.id)

        server_url: str = os.environ.get('SERVER_URL', None)

        if server_url is None:
            raise ValueError('No SERVER_URL variable found, cannot perform aggregation')

        server_url = server_url.rstrip('/')

        LOGGER.info(f'beginning aggregation task={task_id} for artifact_id={artifact_id}')

        res = requests.get(
            f'{server_url}/files/artifact/{artifact_id}',
            headers={'Authorization': f'Bearer {token}'},
        )

        res.raise_for_status()

        artifact = Artifact(**res.json())

        model_name = artifact.model.name

        if model_name == 'FederatedRandomForestClassifier':
            agg = AggregatorRandomForestClassifier()

        else:
            raise ValueError(f'Unsupported model: {model_name}')

        base_model = None

        for model_id in model_ids:
            LOGGER.info(f'requesting {model_id}')
            res = requests.get(f'{server_url}/file/model/{model_id}')

            res.raise_for_status()

            model = pickle.loads(res.content)

            if base_model is None:
                base_model = model
            else:
                base_model = agg.aggregate(artifact.model.strategy, base_model, model)

        LOGGER.info(f'aggregated {len(model_ids)} model(s)')

        res = requests.post(
            f'{server_url}/files/model/{artifact_id}',
            headers={'Authorization': f'Bearer {token}'},
            files={'file': pickle.dumps(base_model)}
        )

        res.raise_for_status()
    except requests.HTTPError as e:
        LOGGER.error(f'{e}')
        LOGGER.exception(e)
