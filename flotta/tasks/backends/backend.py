from typing import Any

from ferdelance.logging import get_logger
from ferdelance.tasks.jobs import Heartbeat, Execution

LOGGER = get_logger(__name__)


class Backend:
    def __init__(self) -> None:
        super().__init__()

    def start_heartbeat(self, component_id: str, remote_id: str, remote_key: str) -> None:
        LOGGER.info(f"component={component_id}: start heartbeat")

        client = Heartbeat.remote(
            component_id,
            remote_id,
            remote_key,
        )

        client.run.remote()  # type: ignore

    def start_exec(
        self,
        artifact_id: str,
        job_id: str,
        component_id: str,
        private_key: str,
        scheduler_id: str,
        scheduler_url: str,
        scheduler_public_key: str,
        datasources: list[dict[str, Any]],
    ) -> None:
        LOGGER.info(f"artifact={artifact_id}: scheduling job={job_id}")

        actor_handler = Execution.remote(
            component_id,
            artifact_id,
            job_id,
            scheduler_id,
            scheduler_url,
            scheduler_public_key,
            private_key,
            datasources,
        )

        task_handler = actor_handler.run.remote()  # type: ignore

        LOGGER.info(f"artifact={artifact_id}: started job={job_id}")

        return task_handler
