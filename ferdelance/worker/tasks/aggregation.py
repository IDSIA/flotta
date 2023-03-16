from typing import Any

from ferdelance.config import conf
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import rebuild_model, Model, GenericModel
from ferdelance.schemas.estimators import rebuild_estimator, Estimator, GenericEstimator
from ferdelance.worker.celery import worker

from celery import Task

import logging
import pickle
import requests

LOGGER = logging.getLogger(__name__)


class AggregationTask(Task):
    def __init__(self) -> None:
        super().__init__()

        self.token: str = ""
        self.server: str = ""
        self.artifact_id: str = ""

    def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)

    def setup(self, artifact_id: str, token: str) -> None:
        self.server = conf.server_url()
        self.token = token
        self.artifact_id = artifact_id

    def headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def get_artifact(self) -> Artifact:
        LOGGER.info(f"artifact_id={self.artifact_id}: fetching artifact")

        res = requests.get(
            f"{self.server}/worker/artifact/{self.artifact_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        return Artifact(**res.json())

    def get_partial(self, result_id: str) -> Any:
        LOGGER.info(f"artifact_id={self.artifact_id}: requesting partial result_id={result_id}")

        res = requests.get(
            f"{self.server}/worker/result/{result_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        return pickle.loads(res.content)

    def post_result(self, base: Any) -> None:
        LOGGER.info(f"artifact_id={self.artifact_id}: posting aggregated result")

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

        LOGGER.info(f"artifact_id={self.artifact_id}: aggregated {len(result_ids)} estimator(s)")

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

        LOGGER.info(f"artifact_id={self.artifact_id}: aggregated {len(result_ids)} model(s)")

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
            LOGGER.error(f"artifact_id={self.artifact_id}: {e}")
            LOGGER.exception(e)


@worker.task(
    ignore_result=False,
    bind=True,
    base=AggregationTask,
)
def aggregation(self: AggregationTask, token: str, artifact_id: str, result_ids: list[str]) -> None:
    try:
        task_id: str = str(self.request.id)

        LOGGER.info(f"artifact_id={artifact_id}: beginning aggregation task={task_id}")

        self.setup(artifact_id, token)
        self.aggregate(result_ids)

    except Exception as e:
        LOGGER.error(f"artifact_id={self.artifact_id}: {e}")
        LOGGER.exception(e)
