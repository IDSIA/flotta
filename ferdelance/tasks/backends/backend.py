from ferdelance.config import get_logger
from ferdelance.schemas.tasks import TaskArguments
from ferdelance.tasks.jobs import EstimationJob, AggregatingJob, TrainingJob
from ferdelance.tasks.jobs.routes import EncryptRouteService

LOGGER = get_logger(__name__)


class Backend:
    def __init__(self) -> None:
        super().__init__()

    def start_aggregation(self, args: TaskArguments) -> None:
        LOGGER.info(f"artifact_id={args.artifact_id}: scheduling aggregation task with job_id={args.job_id}")

        actor_handler = AggregatingJob.remote(
            args.artifact_id,
            args.job_id,
            EncryptRouteService(args.server_url, args.token, args.private_key, args.server_public_key),
        )
        task_handler = actor_handler.run.remote()

        LOGGER.info(f"artifact_id={args.artifact_id}: started task with job_id={args.job_id}")

    def start_training(self, args: TaskArguments) -> None:
        LOGGER.info(f"artifact_id={args.artifact_id}: scheduling training task with job_id={args.job_id}")

        actor_handler = TrainingJob.remote(
            args.artifact_id,
            args.job_id,
            EncryptRouteService(args.server_url, args.token, args.private_key, args.server_public_key),
            args.workdir,
            args.datasources,
        )
        task_handler = actor_handler.run.remote()

        LOGGER.info(f"artifact_id={args.artifact_id}: started task with job_id={args.job_id}")

    def start_estimation(self, args: TaskArguments) -> None:
        LOGGER.info(f"artifact_id={args.artifact_id}: scheduling training task with job_id={args.job_id}")

        actor_handler = EstimationJob.remote(
            args.artifact_id,
            args.job_id,
            EncryptRouteService(args.server_url, args.token, args.private_key, args.server_public_key),
            args.workdir,
            args.datasources,
        )
        task_handler = actor_handler.run.remote()

        LOGGER.info(f"artifact_id={args.artifact_id}: started task with job_id={args.job_id}")
