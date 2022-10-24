from ferdelance_shared.schemas import Artifact
from ferdelance_shared.models import FederatedRandomForestClassifier

from ..celery import worker

import logging
import os
import pickle
import requests

LOGGER = logging.getLogger(__name__)


def headers(token):
    return {'Authorization': f'Bearer {token}'}


@worker.task(
    ignore_result=False,
    bind=True,
)
def aggregation(self, token: str, artifact_id: str, model_ids: list[str]) -> None:
    try:
        task_id: str = str(self.request.id)

        server_url = os.environ.get('SERVER_URL', 'http://server').rstrip('/')
        server_port = os.environ.get('SERVER_PORT', '1456')

        server = f'{server_url}:{server_port}'

        LOGGER.debug(f'using server {server}')

        LOGGER.info(f'beginning aggregation task={task_id} for artifact_id={artifact_id}')

        res = requests.get(
            f'{server}/worker/artifact/{artifact_id}',
            headers=headers(token),
        )

        res.raise_for_status()

        artifact = Artifact(**res.json())

        model_name = artifact.model.name

        if model_name == 'FederatedRandomForestClassifier':
            agg = FederatedRandomForestClassifier()

        else:
            raise ValueError(f'Unsupported model: {model_name}')

        base_model = None

        for model_id in model_ids:
            LOGGER.info(f'requesting {model_id}')

            res = requests.get(
                f'{server}/worker/model/{model_id}',
                headers=headers(token),
            )

            res.raise_for_status()

            model = pickle.loads(res.content)

            if base_model is None:
                base_model = model
            else:
                base_model = agg.aggregate(artifact.model.strategy, base_model, model)

        LOGGER.info(f'aggregated {len(model_ids)} model(s)')

        res = requests.post(
            f'{server}/worker/model/{artifact_id}',
            headers=headers(token),
            files={'file': pickle.dumps(base_model)}
        )

        res.raise_for_status()
    except requests.HTTPError as e:
        LOGGER.error(f'{e}')
        LOGGER.exception(e)
