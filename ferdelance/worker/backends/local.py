from ferdelance.config import conf
from ferdelance.extra import extra
from ferdelance.schemas.worker import TaskArguments
from ferdelance.worker.jobs.services import TrainingJobService, EstimationJobService, AggregatingJobService
from ferdelance.worker.processes import GenericProcess
from .remote import RemoteBackend

from multiprocessing import JoinableQueue

import logging
from uuid import uuid4

LOGGER = logging.getLogger(__name__)


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
