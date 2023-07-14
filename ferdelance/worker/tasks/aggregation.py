from typing import Any

from ferdelance.config import conf
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import GenericModel
from ferdelance.schemas.estimators import GenericEstimator
from ferdelance.schemas.errors import WorkerJobError
from ferdelance.schemas.worker import WorkerTask
from ferdelance.worker.celery import worker

from celery import Task

import json
import logging
import pickle
import requests
import traceback


class AggregationRouter:
    def __init__(self, token: str) -> None:
        self.server = conf.server_url()
        self.token = token

    def headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def get_task(self, job_id: str) -> WorkerTask:
        logging.info(f"job_id={job_id}: fetching task data")

        res = requests.get(
            f"{self.server}/worker/task/{job_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        return WorkerTask(**res.json())

    def get_result(self, artifact_id: str, result_id: str) -> Any:
        logging.info(f"artifact_id={artifact_id}: requesting partial result_id={result_id}")

        res = requests.get(
            f"{self.server}/worker/result/{result_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        return pickle.loads(res.content)

    def post_result(self, artifact_id: str, job_id: str, content: Any) -> None:
        logging.info(f"artifact_id={artifact_id}: posting aggregated result for job_id={job_id}")

        res = requests.post(
            f"{self.server}/worker/result/{job_id}",
            headers=self.headers(),
            files={
                "file": pickle.dumps(content),
            },
        )

        res.raise_for_status()

    def post_error(self, job_id: str, error: WorkerJobError) -> None:
        logging.info(f"job_id={job_id}: posting error")

        res = requests.post(
            f"{self.server}/worker/error/",
            headers=self.headers(),
            data=json.dumps(error.dict()),
        )

        res.raise_for_status()


class AggregationTask(Task):
    abstract = True

    def __init__(self) -> None:
        super().__init__()

        self.router: AggregationRouter = None  # type: ignore

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logging.critical(f"{task_id} failed: exc={exc!r}")
        logging.critical(f"{task_id} failed: args={args!r}")
        logging.critical(f"{task_id} failed: kwargs={kwargs!r}")
        logging.critical(f"{task_id} failed: extra_info={einfo!r}")

    def setup(self, token: str) -> None:
        self.router = AggregationRouter(token)

    def get_task(self, job_id: str) -> WorkerTask:
        task = self.router.get_task(job_id)

        if not task.artifact.id:
            raise ValueError("worker: artifact_id not found")

        return task

    def aggregate_estimator(self, artifact: Artifact, result_ids: list[str]) -> GenericEstimator:
        agg = artifact.get_estimator()

        base: Any = None

        for result_id in result_ids:
            partial: GenericEstimator = self.router.get_result(artifact.id, result_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(base, partial)

        logging.info(f"artifact_id={artifact.id}: aggregated {len(result_ids)} estimator(s)")

        return base

    def aggregate_model(self, artifact: Artifact, result_ids: list[str]) -> GenericModel:
        agg = artifact.get_model()
        strategy = artifact.get_strategy()

        base: Any = None

        for result_id in result_ids:
            partial: GenericModel = self.router.get_result(artifact.id, result_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(strategy, base, partial)

        logging.info(f"artifact_id={artifact.id}: aggregated {len(result_ids)} model(s)")

        return base

    def aggregate(self, job_id: str):
        try:
            logging.debug(f"using server {conf.server_url()}")

            task: WorkerTask = self.get_task(job_id)

            artifact: Artifact = task.artifact
            result_ids: list[str] = task.result_ids

            if artifact.is_estimation():
                base = self.aggregate_estimator(artifact, result_ids)

            elif artifact.is_model():
                base = self.aggregate_model(artifact, result_ids)

            else:
                raise ValueError(f"Unsupported artifact_id={job_id}")

            self.router.post_result(artifact.id, job_id, base)

        except requests.HTTPError as e:
            logging.error(f"artifact_id={job_id}: {e}")
            logging.exception(e)
            raise e


@worker.task(
    ignore_result=True,
    bind=True,
    base=AggregationTask,
)
def aggregation(self: AggregationTask, token: str, job_id: str) -> None:
    task_id: str = str(self.request.id)
    try:
        logging.info(f"worker: beginning aggregation task={task_id}")

        self.setup(token)
        self.aggregate(job_id)

    except Exception as e:
        logging.error(f"task_id={task_id}: job_id={job_id}: {e}")
        logging.exception(e)

        error = WorkerJobError(
            job_id=job_id,
            message=str(e),
            stack_trace="".join(traceback.TracebackException.from_exception(e).format()),
        )

        self.router.post_error(job_id, error)
