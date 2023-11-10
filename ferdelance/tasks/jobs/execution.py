from typing import Any
from ferdelance.config.config import DataSourceConfiguration
from ferdelance.core.environment.core import Environment
from ferdelance.tasks.services.routes import EncryptRouteService, RouteService

import ray


@ray.remote
class Execution:
    def __init__(
        self,
        # the worker node id
        component_id: str,
        # id of the artifact to run
        artifact_id: str,
        # id of the job to download
        job_id: str,
        # url of the remote node to use (if localhost, same node)
        node_url: str,
        # this node private key (for communication)
        private_key: str,
        # public key of the remote node (if none, same node)
        node_public_key: str,
        datasources: list[dict[str, Any]],
        # if true, we don't have to send data to the remote server
        local_node: bool = False,
    ) -> None:
        self.component_id: str = component_id
        self.artifact_id: str = artifact_id
        self.job_id: str = job_id

        self.datasources: list[DataSourceConfiguration] = [DataSourceConfiguration(**ds) for ds in datasources]

        self.local_node: bool = local_node

        self.routes_service: RouteService = EncryptRouteService(
            self.component_id,
            node_url,
            private_key,
            node_public_key,
        )

    def __repr__(self) -> str:
        return f"Job artifact={self.artifact_id} job={self.job_id}"

    def run(self):
        task = self.routes_service.get_task_data(self.artifact_id, self.job_id)

        env = Environment()

        # get required resources
        env = task.job.step.pull(env, roures_service)

        # apply work from step
        env = task.job.step.step(env)

        # send forward the produced resources
        env = task.job.step.push(env, roures_service)
