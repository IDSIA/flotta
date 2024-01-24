from typing import Any

from ferdelance.config import DataSourceConfiguration
from ferdelance.config.config import DataSourceStorage
from ferdelance.logging import get_logger
from ferdelance.tasks.services import RouteService, ExecutionService
from ferdelance.tasks.tasks import Task, TaskError

from pathlib import Path

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
        # id of the job to fetch
        job_id: str,
        # component id of the scheduler node (if localhost, same as component_id)
        scheduler_id: str,
        # url of the remote node to use (if localhost, same node)
        scheduler_url: str,
        # public key of the remote node (if none, same node)
        scheduler_public_key: str,
        # this node private key (for communication)
        private_key: str,
        datasources: list[dict[str, Any]],
        # if true, we don't have to send data to the remote server
        scheduler_is_local: bool,
    ) -> None:
        self.component_id: str = component_id
        self.artifact_id: str = artifact_id
        self.job_id: str = job_id

        self.scheduler_id: str = scheduler_id
        self.scheduler_url: str = scheduler_url
        self.scheduler_public_key: str = scheduler_public_key
        self.private_key: str = private_key

        self.data = DataSourceStorage([DataSourceConfiguration(**ds) for ds in datasources])

        self.scheduler_is_local: bool = scheduler_is_local

    def __repr__(self) -> str:
        return f"Job={self.job_id} artifact={self.artifact_id}"

    def run(self):
        artifact_id: str = self.artifact_id
        job_id: str = self.job_id

        LOGGER.info(f"JOB job={job_id}: received task")

        scheduler = RouteService(
            self.component_id,
            self.private_key,
            self.scheduler_id,
            self.scheduler_url,
            self.scheduler_public_key,
            self.scheduler_is_local,
        )

        try:
            task: Task = scheduler.get_task_data(artifact_id, job_id)

            LOGGER.info(f"JOB job={job_id}: obtained task")

            es = ExecutionService(task, self.data, self.component_id)

            es.load()

            # get required resources
            LOGGER.info(f"JOB job={job_id}: collecting {len(task.required_resources)} resource(s)")

            for resource in task.required_resources:
                if resource.available_locally:
                    if resource.local_path is None:
                        raise ValueError("Resource available locally has no path!")

                    LOGGER.info(f"JOB job={job_id}: obtaining resource locally")

                    res_path = Path(resource.local_path)

                else:
                    node = RouteService(
                        self.component_id,
                        self.private_key,
                        resource.component_id,
                        resource.component_url,
                        resource.component_public_key,
                    )

                    LOGGER.info(
                        f"JOB job={job_id}: obtaining resource from node={resource.component_id} url={resource.component_url}"
                    )

                    # path to resource saved locally
                    res_path: Path = node.get_resource(
                        resource.component_id,
                        resource.artifact_id,
                        resource.job_id,
                        resource.resource_id,
                        resource.iteration,
                    )

                es.env.add_resource(resource.resource_id, res_path)

            # apply work from step
            LOGGER.info(f"JOB job={job_id}: starting execution")

            es.run()

            if es.env.products is None:
                LOGGER.error(f"JOB job={job_id}: nothing has been produced!")
                raise ValueError("Produced resource not available")

            # send forward the produced resources
            LOGGER.info(
                f"JOB job={job_id}: sending produced resource={task.produced_resource_id} "
                f"to {len(task.next_nodes)} node(s)"
            )

            for next_node in task.next_nodes:
                is_local = next_node.target_id == self.component_id

                LOGGER.info(
                    f"JOB job={job_id}: sending resource={task.produced_resource_id} to "
                    f"component={next_node.target_id} "
                    f"via url={next_node.target_url} "
                    f"is_local={is_local}"
                )
                node = RouteService(
                    self.component_id,
                    self.private_key,
                    next_node.target_id,
                    next_node.target_url,
                    next_node.target_public_key,
                    is_local,
                )

                # TODO: consider proxy!!!

                # if next_node.target_id != self.scheduler_id:
                #     LOGGER.info(f"JOB job={job_id}: target node={next_node.target_id} is not the ")
                #     node.exc.set_remote_key(next_node.target_id, next_node.target_public_key)
                #     node.exc.set_proxy_key(self.scheduler_public_key)

                if is_local:
                    node.post_resource(artifact_id, job_id, task.produced_resource_id)

                else:
                    node.post_resource(artifact_id, job_id, task.produced_resource_id, es.env.product_path())

            scheduler.post_done(artifact_id, job_id)

            LOGGER.info(f"JOB job={job_id}: execution completed with success")

        except Exception as e:
            LOGGER.error(f"JOB job={job_id}: execution failed")
            LOGGER.exception(e)

            scheduler.post_error(
                job_id,
                TaskError(
                    job_id=job_id,
                    message=f"{e}",
                    stack_trace="".join(traceback.TracebackException.from_exception(e).format()),
                ),
            )
