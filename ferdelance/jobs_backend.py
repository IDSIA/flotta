from abc import ABC, abstractclassmethod

from ferdelance.config import conf
from ferdelance.extra import extra
from ferdelance.worker.tasks.aggregation import aggregation
from ferdelance.worker.tasks.training import training
from ferdelance.worker.tasks.estimate import estimate
from ferdelance.worker.processes import ClientWorker

from celery.result import AsyncResult
from multiprocessing import JoinableQueue
from uuid import uuid4

import logging

LOGGER = logging.getLogger(__name__)


class Backend(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractclassmethod
    def start_aggregation(self, token: str, job_id: str, artifact_id: str) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def start_training(self, token: str, job_id: str, artifact_id: str) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def start_estimation(self, token: str, job_id: str, artifact_id: str) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def stop_backend(self):
        raise NotImplementedError()


class RemoteBackend(Backend):
    """This backend is used to schedule tasks for celery."""

    def __init__(self) -> None:
        super().__init__()

    def start_aggregation(self, token: str, job_id: str, artifact_id: str) -> str:
        LOGGER.info(f"artifact_id={artifact_id}: started aggregation task with job_id={job_id}")
        task: AsyncResult = aggregation.apply_async(
            args=[
                token,
                job_id,
            ],
        )
        task_id = str(task.task_id)
        LOGGER.info(
            f"artifact_id={artifact_id}: scheduled task with job_id={job_id} celery_id={task_id} status={task.status}"
        )
        return task_id

    def start_training(self, token: str, job_id: str, artifact_id: str) -> str:
        LOGGER.info(f"artifact_id={artifact_id}: started training task with job_id={job_id}")
        task: AsyncResult = training.apply_async(
            args=[
                token,
                job_id,
            ],
        )
        task_id = str(task.task_id)
        LOGGER.info(
            f"artifact_id={artifact_id}: scheduled task with job_id={job_id} celery_id={task_id} status={task.status}"
        )
        return task_id

    def start_estimation(self, token: str, job_id: str, artifact_id: str) -> str:
        LOGGER.info(f"artifact_id={artifact_id}: started training task with job_id={job_id}")
        task: AsyncResult = estimate.apply_async(
            args=[
                token,
                job_id,
            ],
        )
        task_id = str(task.task_id)
        LOGGER.info(
            f"artifact_id={artifact_id}: scheduled task with job_id={job_id} celery_id={task_id} status={task.status}"
        )
        return task_id

    def stop_backend(self):
        LOGGER.info("BACKEND: received stop order, nothing to do")
        return


class LocalBackend(RemoteBackend):
    """This backend is used to schedule tasks on a local queue."""

    def __init__(self) -> None:
        self.training_queue: JoinableQueue | None = extra.training_queue
        self.estimation_queue: JoinableQueue | None = extra.estimation_queue
        self.aggregation_queue: JoinableQueue | None = extra.aggregation_queue

        # TODO: start extra.aggregation_workers

        if not extra.training_workers:
            for i in range(conf.N_TRAIN_WORKER):
                w = ClientWorker(
                    self.config,
                    f"trainer-{i}",
                    self.training_queue,
                )
                w.start()
                extra.training_workers.append(w)

        if not extra.estimation_workers:
            for i in range(conf.N_ESTIMATE_WORKER):
                w = ClientWorker(
                    self.config,
                    f"estimator-{i}",
                    self.estimation_queue,
                )
                w.start()
                extra.estimation_workers.append(w)

    def start_aggregation(self, token: str, job_id: str, artifact_id: str) -> str:
        LOGGER.info(f"LOCAL: starting local aggregation for artifact_id={artifact_id}")

        if self.aggregation_queue is None:
            raise ValueError("LOCAL: Could not start aggregation without a queue!")

        self.aggregation_queue.put((token, job_id))

        return f"local-{str(uuid4())}"

    def start_training(self, token: str, job_id: str, artifact_id: str) -> str:
        LOGGER.info(f"LOCAL: start training with job_id={job_id} for artifact_id={artifact_id}")

        if self.training_queue is None:
            raise ValueError("LOCAL: Could not start training without a queue!")

        self.training_queue.put((token, job_id))

        return f"local-{str(uuid4())}"

    def start_estimation(self, token: str, job_id: str, artifact_id: str) -> str:
        LOGGER.info(f"LOCAL: start estimate with job_id={job_id} for artifact_id={artifact_id}")

        if self.estimation_queue is None:
            raise ValueError("LOCAL: Could not start estimation without a queue!")

        self.estimation_queue.put((token, job_id))

        return f"local-{str(uuid4())}"

    def stop_backend(self):
        LOGGER.info("LOCAL: received stop order, nothing to do")

        if self.aggregation_queue is not None:
            self.aggregation_queue.put(None)

        if self.training_queue is not None:
            self.training_queue.put(None)

        if self.estimation_queue is not None:
            self.estimation_queue.put(None)


def get_jobs_backend() -> Backend:
    if conf.STANDALONE:
        return LocalBackend()
    return RemoteBackend()
