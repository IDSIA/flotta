from typing import Any

from ferdelance.config import conf
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import GenericModel
from ferdelance.schemas.estimators import GenericEstimator
from ferdelance.schemas.errors import ErrorArtifact
from ferdelance.schemas.worker import WorkerTask
from ferdelance.worker.celery import worker

from celery import Task

import json
import logging
import pickle
import requests
import traceback


class AggregationTask(Task):
    abstract = True

    def __init__(self) -> None:
        super().__init__()

        self.token: str = ""
        self.server: str = ""
        self.job_id: str = ""
        self.artifact_id: str = ""

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logging.critical(f"{task_id} failed: exc={exc!r}")
        logging.critical(f"{task_id} failed: args={args!r}")
        logging.critical(f"{task_id} failed: kwargs={kwargs!r}")
        logging.critical(f"{task_id} failed: extra_info={einfo!r}")

    def setup(self, job_id: str, token: str) -> None:
        self.server = conf.server_url()
        self.token = token
        self.job_id = job_id

    def headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def get_task(self) -> Artifact:
        logging.info(f"job_id={self.job_id}: fetching task data")

        res = requests.get(
            f"{self.server}/worker/task/{self.job_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        task = WorkerTask(**res.json())

        if task.artifact.id is None:
            raise ValueError("worker: artifact_id not found")

        self.artifact_id = task.artifact.id

        return task.artifact

    def get_partial(self, result_id: str) -> Any:
        logging.info(f"job_id={self.job_id}: requesting partial result_id={result_id}")

        res = requests.get(
            f"{self.server}/worker/result/{result_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        return pickle.loads(res.content)

    def post_result(self, base: Any) -> None:
        logging.info(f"job_id={self.job_id}: posting aggregated result")

        res = requests.post(
            f"{self.server}/worker/result/{self.job_id}",
            headers=self.headers(),
            files={
                "file": pickle.dumps(base),
            },
        )

        res.raise_for_status()

    def post_error(self, error: ErrorArtifact) -> None:
        logging.info(f"job_id={self.job_id}: posting error")

        res = requests.post(
            f"{self.server}/worker/error/",
            headers=self.headers(),
            data=json.dumps(error.dict()),
        )

        res.raise_for_status()

    def aggregate_estimator(self, artifact: Artifact, result_ids: list[str]) -> GenericEstimator:
        agg = artifact.get_estimator()

        base: Any = None

        for result_id in result_ids:
            partial: GenericEstimator = self.get_partial(result_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(base, partial)

        logging.info(f"artifact_id={self.job_id}: aggregated {len(result_ids)} estimator(s)")

        return base

    def aggregate_model(self, artifact: Artifact, result_ids: list[str]) -> GenericModel:
        agg = artifact.get_model()
        strategy = artifact.get_strategy()

        base: Any = None

        for result_id in result_ids:
            partial: GenericModel = self.get_partial(result_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(strategy, base, partial)

        logging.info(f"artifact_id={self.job_id}: aggregated {len(result_ids)} model(s)")

        return base

    def aggregate(self, result_ids: list[str]):
        try:
            server = conf.server_url()

            logging.debug(f"using server {server}")

            artifact: Artifact = self.get_task()

            if artifact.is_estimation():
                base = self.aggregate_estimator(artifact, result_ids)

            elif artifact.is_model():
                base = self.aggregate_model(artifact, result_ids)

            else:
                raise ValueError(f"Unsupported artifact_id={self.job_id}")

            self.post_result(base)

        except requests.HTTPError as e:
            logging.error(f"artifact_id={self.job_id}: {e}")
            logging.exception(e)


@worker.task(
    ignore_result=True,
    bind=True,
    base=AggregationTask,
)
def aggregation(self: AggregationTask, token: str, job_id: str, result_ids: list[str]) -> None:
    task_id: str = str(self.request.id)
    try:
        logging.info(f"worker: beginning aggregation task={task_id}")

        self.setup(job_id, token)
        self.aggregate(result_ids)

    except Exception as e:
        logging.error(f"task_id={task_id}: job_id={self.job_id} artifact_id={self.artifact_id}: {e}")
        logging.exception(e)

        error = ErrorArtifact(
            artifact_id=self.artifact_id,
            job_id=self.job_id,
            message=str(e),
            stack_trace="".join(traceback.TracebackException.from_exception(e).format()),
        )

        self.post_error(error)
