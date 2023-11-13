from typing import Any

from ferdelance.config import DataSourceConfiguration
from ferdelance.core.environment import Environment
from ferdelance.tasks.services import RouteService
from ferdelance.tasks.tasks import Task, TaskError

import ray
import traceback


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
        scheduler_url: str,
        # public key of the remote node (if none, same node)
        scheduler_public_key: str,
        # this node private key (for communication)
        private_key: str,
        datasources: list[dict[str, Any]],
        # if true, we don't have to send data to the remote server
        scheduler_is_local: bool = False,
    ) -> None:
        self.component_id: str = component_id
        self.artifact_id: str = artifact_id
        self.job_id: str = job_id

        self.scheduler_url: str = scheduler_url
        self.scheduler_public_key: str = scheduler_public_key
        self.private_key: str = private_key

        self.datasources: list[DataSourceConfiguration] = [DataSourceConfiguration(**ds) for ds in datasources]

        self.scheduler_is_local: bool = scheduler_is_local

    def __repr__(self) -> str:
        return f"Job artifact={self.artifact_id} job={self.job_id}"

    def run(self):
        scheduler = RouteService(
            self.component_id,
            self.private_key,
            self.scheduler_url,
            self.scheduler_public_key,
            self.scheduler_is_local,
        )

        try:
            task: Task = scheduler.get_task_data(self.artifact_id, self.job_id)

            env = Environment()

            # get required resources
            for resource in task.task_resources:
                node = RouteService(
                    self.component_id,
                    self.private_key,
                    resource.url,
                    resource.public_key,
                )

                # path to resource saved locally
                env[resource.resource_id] = node.get_resource(
                    resource.artifact_id,
                    resource.job_id,
                    resource.resource_id,
                )

            # apply work from step
            env = task.run(env)

            # send forward the produced resources
            for next_node in task.next_nodes:
                node = RouteService(
                    self.component_id,
                    self.private_key,
                    next_node.url,
                    next_node.public_key,
                    next_node.is_local,
                )

                node.post_resource(self.artifact_id, self.job_id, env["resource_path"])

            scheduler.post_done(self.artifact_id, self.job_id)

        except Exception as e:
            scheduler.post_error(
                self.artifact_id,
                self.job_id,
                TaskError(
                    job_id=self.job_id,
                    message=f"{e}",
                    stack_trace="".join(traceback.TracebackException.from_exception(e).format()),
                ),
            )
