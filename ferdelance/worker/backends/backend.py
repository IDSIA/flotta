from ferdelance.schemas.tasks import TaskArguments
from ferdelance.worker.jobs import EstimationJob, AggregatingJob, TrainingJob

from ray import ObjectRef

import logging

from ferdelance.worker.jobs.routes import EncryptRouteService

LOGGER = logging.getLogger(__name__)


class Backend:
    def __init__(self) -> None:
        super().__init__()

    def start_aggregation(self, args: TaskArguments) -> None:
        LOGGER.info(f"artifact_id={args.artifact_id}: scheduling aggregation task with job_id={args.job_id}")

        actor_handler: ObjectRef[AggregatingJob] = AggregatingJob.remote(
            args.artifact_id,
            args.job_id,
            EncryptRouteService(args.server_url, args.token, args.private_key_location, args.server_public_key),
        )
        task_handler = actor_handler.run.remote()

        LOGGER.info(f"artifact_id={args.artifact_id}: started task with job_id={args.job_id}")

    def start_training(self, args: TaskArguments) -> None:
        LOGGER.info(f"artifact_id={args.artifact_id}: scheduling training task with job_id={args.job_id}")

        actor_handler: ObjectRef[TrainingJob] = TrainingJob.remote(
            args.artifact_id,
            args.job_id,
            EncryptRouteService(args.server_url, args.token, args.private_key_location, args.server_public_key),
            args.workdir,
            args.datasources,
        )
        task_handler = actor_handler.run.remote()

        LOGGER.info(f"artifact_id={args.artifact_id}: started task with job_id={args.job_id}")

    def start_estimation(self, args: TaskArguments) -> None:
        LOGGER.info(f"artifact_id={args.artifact_id}: scheduling training task with job_id={args.job_id}")

        actor_handler: ObjectRef[EstimationJob] = EstimationJob.remote(
            args.artifact_id,
            args.job_id,
            EncryptRouteService(args.server_url, args.token, args.private_key_location, args.server_public_key),
            args.workdir,
            args.datasources,
        )
        task_handler = actor_handler.run.remote()

        LOGGER.info(f"artifact_id={args.artifact_id}: started task with job_id={args.job_id}")
