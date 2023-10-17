from typing import Any

from ferdelance.logging import get_logger
from ferdelance.tasks.jobs.aggregation import AggregatingJob
from ferdelance.tasks.jobs.estimating import EstimationJob
from ferdelance.tasks.jobs.heartbeat import Heartbeat
from ferdelance.tasks.jobs.initialization import InitializationJob
from ferdelance.tasks.jobs.training import TrainingJob

LOGGER = get_logger(__name__)


class Backend:
    def __init__(self) -> None:
        super().__init__()

    def start_heartbeat(self, component_id: str, remote_key: str) -> None:
        LOGGER.info(f"component={component_id}: start heartbeat")

        client = Heartbeat.remote(
            component_id,
            remote_key,
        )

        client.run.remote()  # type: ignore

    def start_init(
        self,
        artifact_id: str,
        job_id: str,
        component_id: str,
        private_key: str,
        node_url: str,
        node_public_key: str,
    ) -> None:
        LOGGER.info(f"artifact={artifact_id}: scheduling initialization job={job_id}")

        actor_handler = InitializationJob.remote(
            component_id,
            artifact_id,
            job_id,
            node_url,
            private_key,
            node_public_key,
        )

        task_handler = actor_handler.run.remote()  # type: ignore

        LOGGER.info(f"artifact={artifact_id}: started initialization job={job_id}")

        return task_handler

    def start_aggregation(
        self,
        component_id: str,
        private_key: str,
        node_url: str,
        node_public_key: str,
        job_id: str,
        artifact_id: str,
    ) -> None:
        LOGGER.info(f"artifact={artifact_id}: scheduling aggregation job={job_id}")

        actor_handler = AggregatingJob.remote(
            component_id,
            artifact_id,
            job_id,
            node_url,
            private_key,
            node_public_key,
        )
        task_handler = actor_handler.run.remote()  # type: ignore

        LOGGER.info(f"artifact={artifact_id}: started task with job={job_id}")

        return task_handler

    def start_training(
        self,
        component_id: str,
        private_key: str,
        node_url: str,
        node_public_key: str,
        datasources: list[dict[str, Any]],
        job_id: str,
        artifact_id: str,
    ) -> None:
        LOGGER.info(f"artifact={artifact_id}: scheduling training task with job={job_id}")

        actor_handler = TrainingJob.remote(
            component_id,
            artifact_id,
            job_id,
            node_url,
            private_key,
            node_public_key,
            datasources,
        )
        task_handler = actor_handler.run.remote()  # type: ignore

        LOGGER.info(f"artifact={artifact_id}: started task with job={job_id}")

        return task_handler

    def start_estimation(
        self,
        component_id: str,
        private_key: str,
        node_url: str,
        node_public_key: str,
        datasources: list[dict[str, Any]],
        job_id: str,
        artifact_id: str,
    ) -> None:
        LOGGER.info(f"artifact={artifact_id}: scheduling training task with job={job_id}")

        actor_handler = EstimationJob.remote(
            component_id,
            artifact_id,
            job_id,
            node_url,
            private_key,
            node_public_key,
            datasources,
        )
        task_handler = actor_handler.run.remote()  # type: ignore

        LOGGER.info(f"artifact={artifact_id}: started task with job={job_id}")

        return task_handler
