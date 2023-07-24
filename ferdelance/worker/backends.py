from abc import ABC, abstractclassmethod

from ferdelance.config import conf
from ferdelance.extra import extra
from ferdelance.schemas.worker import TaskArguments
from ferdelance.worker.jobs.services import TrainingJobService, EstimationJobService, AggregatingJobService
from ferdelance.worker.processes import GenericProcess
from ferdelance.worker.tasks.aggregation import aggregation
from ferdelance.worker.tasks.training import training
from ferdelance.worker.tasks.estimate import estimate


from celery.result import AsyncResult
from multiprocessing import JoinableQueue
from uuid import uuid4

import logging

LOGGER = logging.getLogger(__name__)


class Backend(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractclassmethod
    def start_aggregation(self, args: TaskArguments) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def start_training(self, args: TaskArguments) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def start_estimation(self, args: TaskArguments) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def stop_backend(self):
        raise NotImplementedError()


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
        task: AsyncResult = estimate.apply_async(args=[args.dict()])
        task_id = str(task.task_id)
        LOGGER.info(
            f"artifact_id={args.artifact_id}: "
            f"scheduled task with job_id={args.job_id} celery_id={task_id} status={task.status}"
        )
        return task_id

    def stop_backend(self):
        LOGGER.info("BACKEND: received stop order, nothing to do")
        return


class LocalBackend(RemoteBackend):
    """This backend is used to schedule tasks on local queues."""

    def __init__(self) -> None:
        self.training_queue: JoinableQueue | None = extra.training_queue
        self.estimation_queue: JoinableQueue | None = extra.estimation_queue
        self.aggregation_queue: JoinableQueue | None = extra.aggregation_queue

        if not extra.aggregation_workers and self.aggregation_queue is not None:
            for i in range(conf.N_AGGREGATE_WORKER):
                w = GenericProcess(
                    f"trainer-{i}",
                    self.aggregation_queue,
                    AggregatingJobService(),
                )
                w.start()
                extra.aggregation_workers.append(w)

        if not extra.training_workers and self.training_queue is not None:
            for i in range(conf.N_TRAIN_WORKER):
                w = GenericProcess(
                    f"trainer-{i}",
                    self.training_queue,
                    TrainingJobService(),
                )
                w.start()
                extra.training_workers.append(w)

        if not extra.estimation_workers and self.estimation_queue is not None:
            for i in range(conf.N_ESTIMATE_WORKER):
                w = GenericProcess(
                    f"estimator-{i}",
                    self.estimation_queue,
                    EstimationJobService(),
                )
                w.start()
                extra.estimation_workers.append(w)

    def start_aggregation(self, args: TaskArguments) -> str:
        LOGGER.info(f"LOCAL: starting local aggregation for artifact_id={args.artifact_id}")

        if self.aggregation_queue is None:
            raise ValueError("LOCAL: Could not start aggregation without a queue!")

        self.aggregation_queue.put(args.dict())

        return f"local-{str(uuid4())}"

    def start_training(self, args: TaskArguments) -> str:
        LOGGER.info(f"LOCAL: start training with job_id={args.job_id} for artifact_id={args.artifact_id}")

        if self.training_queue is None:
            raise ValueError("LOCAL: Could not start training without a queue!")

        self.training_queue.put(args.dict())

        return f"local-{str(uuid4())}"

    def start_estimation(self, args: TaskArguments) -> str:
        LOGGER.info(f"LOCAL: start estimate with job_id={args.job_id} for artifact_id={args.artifact_id}")

        if self.estimation_queue is None:
            raise ValueError("LOCAL: Could not start estimation without a queue!")

        self.estimation_queue.put(args.dict())

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
