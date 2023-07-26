from ferdelance.schemas.worker import TaskArguments
from ferdelance.worker.tasks import aggregation, training, estimation
from .backend import Backend

from celery.result import AsyncResult

import logging

LOGGER = logging.getLogger(__name__)


class RemoteBackend(Backend):
    """This backend is used to schedule tasks for celery."""

    def __init__(self) -> None:
        super().__init__()

    def start_aggregation(self, args: TaskArguments) -> str:
        LOGGER.info(f"artifact_id={args.artifact_id}: started aggregation task with job_id={args.job_id}")
        task: AsyncResult = aggregation.apply_async(args=[args.dict()])
        task_id = str(task.task_id)
        LOGGER.info(
            f"artifact_id={args.artifact_id}: "
            f"scheduled task with job_id={args.job_id} celery_id={task_id} status={task.status}"
        )
        return task_id

    def start_training(self, args: TaskArguments) -> str:
        LOGGER.info(f"artifact_id={args.artifact_id}: started training task with job_id={args.job_id}")
        task: AsyncResult = training.apply_async(args=[args.dict()])
        task_id = str(task.task_id)
        LOGGER.info(
            f"artifact_id={args.artifact_id}: "
            f"scheduled task with job_id={args.job_id} celery_id={task_id} status={task.status}"
        )
        return task_id

    def start_estimation(self, args: TaskArguments) -> str:
        LOGGER.info(f"artifact_id={args.artifact_id}: started training task with job_id={args.job_id}")
        task: AsyncResult = estimation.apply_async(args=[args.dict()])
        task_id = str(task.task_id)
        LOGGER.info(
            f"artifact_id={args.artifact_id}: "
            f"scheduled task with job_id={args.job_id} celery_id={task_id} status={task.status}"
        )
        return task_id

    def stop_backend(self):
        LOGGER.info("BACKEND: received stop order, nothing to do")
        return
