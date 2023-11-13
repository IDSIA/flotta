from typing import Any

from ferdelance.config import DataSourceConfiguration, config_manager
from ferdelance.config.config import DataSourceStorage
from ferdelance.core.environment import Environment
from ferdelance.logging import get_logger
from ferdelance.tasks.services import RouteService
from ferdelance.tasks.tasks import Task, TaskError

import json
import os
import pandas as pd
import ray
import traceback

LOGGER = get_logger(__name__)


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

        self.data = DataSourceStorage([DataSourceConfiguration(**ds) for ds in datasources])

        self.scheduler_is_local: bool = scheduler_is_local

    def __repr__(self) -> str:
        return f"Job artifact={self.artifact_id} job={self.job_id}"

    def run(self):
        artifact_id: str = self.artifact_id
        job_id: str = self.job_id

        LOGGER.info(f"artifact={artifact_id}: received new task with job={job_id}")

        config = config_manager.get()

        scheduler = RouteService(
            self.component_id,
            self.private_key,
            self.scheduler_url,
            self.scheduler_public_key,
            self.scheduler_is_local,
        )

        try:
            task: Task = scheduler.get_task_data(artifact_id, job_id)

            # creating working folders
            working_folder = os.path.join(config.storage_artifact(artifact_id, task.iteration), f"{job_id}")

            os.makedirs(working_folder, exist_ok=True)

            path_artifact = os.path.join(working_folder, "task.json")

            with open(path_artifact, "w") as f:
                json.dump(task.dict(), f)

            env = Environment()

            # load local data
            dfs: list[pd.DataFrame] = []

            LOGGER.debug(f"artifact={artifact_id}: number of datasources={len(self.data)}")

            for hs in self.data.hashes():
                ds = self.data[hs]

                if not ds:
                    LOGGER.debug(f"artifact={artifact_id}: datasource_hash={hs} invalid")
                    continue

                if not ds.check_token(task.project_id):
                    LOGGER.debug(f"artifact={artifact_id}: datasource_hash={hs} ignored")
                    continue

                LOGGER.debug(f"artifact={artifact_id}: considering datasource_hash={hs}")

                datasource: pd.DataFrame = ds.get()  # TODO: implemented only for files!

                dfs.append(datasource)

            env.df = pd.concat(dfs)  # TODO: do we want it to be loaded in df?

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

                node.post_resource(artifact_id, job_id, env["resource_path"])

            scheduler.post_done(artifact_id, job_id)

        except Exception as e:
            scheduler.post_error(
                artifact_id,
                job_id,
                TaskError(
                    job_id=job_id,
                    message=f"{e}",
                    stack_trace="".join(traceback.TracebackException.from_exception(e).format()),
                ),
            )
