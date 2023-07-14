from ferdelance.config import conf
from ferdelance.standalone.extra import extra
from ferdelance.worker.tasks.aggregation import aggregation

from celery.result import AsyncResult
from multiprocessing import Queue
from uuid import uuid4

import logging

LOGGER = logging.getLogger(__name__)


class RemoteBackend:
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


class LocalBackend(RemoteBackend):
    """This backend is used to schedule tasks on a local queue."""

    def __init__(self) -> None:
        if extra.aggregation_queue is None:
            raise ValueError("Could not run without a queue")

        self.aggregation_queue: Queue = extra.aggregation_queue

    def start_aggregation(self, token: str, job_id: str, artifact_id: str) -> str:
        LOGGER.info(f"standalone: starting local aggregation for artifact_id={artifact_id}")

        self.aggregation_queue.put((token, job_id))

        return f"local-{str(uuid4())}"


def get_aggregation_backend() -> RemoteBackend:
    if conf.STANDALONE:
        return LocalBackend()
    return RemoteBackend()
