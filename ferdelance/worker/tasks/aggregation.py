from typing import Any

from ferdelance.config import conf
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import rebuild_model, Model, GenericModel
from ferdelance.schemas.estimators import rebuild_estimator, Estimator, GenericEstimator
from ferdelance.worker.celery import worker

import logging
import pickle
import requests

LOGGER = logging.getLogger(__name__)


class Aggregator:
    def __init__(self, artifact_id: str, token: str) -> None:
        self.server = conf.server_url()
        self.token = token
        self.artifact_id = artifact_id

    def headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def get_artifact(self) -> Artifact:
        LOGGER.info(f"requesting artifact_id={self.artifact_id}")

        res = requests.get(
            f"{self.server}/worker/artifact/{self.artifact_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        return Artifact(**res.json())

    def get_partial(self, result_id: str) -> Any:
        LOGGER.info(f"requesting result_id={result_id}")

        res = requests.get(
            f"{self.server}/worker/result/{result_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        return pickle.loads(res.content)

    def post_result(self, base: Any) -> None:
        LOGGER.info(f"posting result for artifact_id={self.artifact_id}")

        server = conf.server_url()

        res = requests.post(
            f"{self.server}/worker/result/{self.artifact_id}",
            headers=self.headers(),
            files={
                "file": pickle.dumps(base),
            },
        )

        res.raise_for_status()

    def aggregate_estimator(self, estimator: Estimator, result_ids: list[str]) -> GenericEstimator:
        agg = rebuild_estimator(estimator)

        base: Any = None

        for result_id in result_ids:
            partial: GenericEstimator = self.get_partial(result_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(base, partial)

        LOGGER.info(f"aggregated {len(result_ids)} model(s)")

        return base

    def aggregate_model(self, model: Model, result_ids: list[str]) -> GenericModel:
        agg = rebuild_model(model)

        base: Any = None

        for result_id in result_ids:
            partial: GenericModel = self.get_partial(result_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(model.strategy, base, partial)

        LOGGER.info(f"aggregated {len(result_ids)} model(s)")

        return base

    def aggregate(self, result_ids: list[str]):
        try:
            server = conf.server_url()

            LOGGER.debug(f"using server {server}")

            artifact = self.get_artifact()

            if artifact.estimate is not None:
                base = self.aggregate_estimator(artifact.estimate, result_ids)

            elif artifact.model is not None:
                base = self.aggregate_model(artifact.model, result_ids)

            else:
                raise ValueError(f"Unsupported artifact_id={self.artifact_id}")  # manage

            self.post_result(base)

        except requests.HTTPError as e:
            LOGGER.error(f"{e}")
            LOGGER.exception(e)


@worker.task(
    ignore_result=False,
    bind=True,
)
def aggregation(self, token: str, artifact_id: str, result_ids: list[str]) -> None:
    task_id: str = str(self.request.id)

    LOGGER.info(f"beginning aggregation task={task_id} for artifact_id={artifact_id}")

    a = Aggregator(artifact_id, token)
    a.aggregate(result_ids)
