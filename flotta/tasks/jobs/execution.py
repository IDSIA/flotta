from typing import Any

from ferdelance.config import config_manager
from ferdelance.logging import get_logger
from ferdelance.tasks.services import RouteService
from ferdelance.tasks.services.execution import TaskExecutionService

import ray

LOGGER = get_logger(__name__)


@ray.remote
class Execution:
    def __init__(
        self,
        component_id: str,
        artifact_id: str,
        job_id: str,
        scheduler_id: str,
        scheduler_url: str,
        scheduler_public_key: str,
        private_key: str,
        datasources: list[dict[str, Any]],
    ) -> None:
        """Task that is capable of executing jobs.

        Args:
            component_id (str):
                The worker node id.
            artifact_id (str):
                Id of the artifact to run.
            job_id (str):
                Id of the job to fetch from the scheduler.
            scheduler_id (str):
                Component id of the scheduler node. In case the scheduler is local
                (localhost), this argument is the same as `component_id`.
            scheduler_url (str):
                URL of the remote node to use. When the scheduler is on the same
                node, it is set to `http://localhost`.
            scheduler_public_key (str):
                Public key of the remote node.
            private_key (str):
                The private key in string format for this node.
            datasources (list[dict[str, Any]]):
                List of maps to available datasources. Can be obtained from node configuration.
            scheduler_is_local (bool):
                Set to True when we don't have to send data to a remote server.
        """
        route_service = RouteService(component_id, private_key)

        config = config_manager.get()

        self.task_executor = TaskExecutionService(
            route_service,
            component_id,
            artifact_id,
            job_id,
            scheduler_id,
            scheduler_url,
            scheduler_public_key,
            datasources,
            config.storage_artifact_dir(),
        )

    def run(self) -> None:
        self.task_executor.run()
