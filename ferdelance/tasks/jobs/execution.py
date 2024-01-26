from typing import Any

from ferdelance.config import DataSourceConfiguration, DataSourceStorage
from ferdelance.logging import get_logger
from ferdelance.tasks.services import RouteService, ExecutionService
from ferdelance.tasks.tasks import Task, TaskError

from pathlib import Path

import ray
import traceback

LOGGER = get_logger(__name__)


class TaskExecutor:
    def __init__(
        self,
        route_service: RouteService,
        component_id: str,
        artifact_id: str,
        job_id: str,
        scheduler_id: str,
        scheduler_url: str,
        scheduler_public_key: str,
        datasources: list[dict[str, Any]],
    ) -> None:
        """Task that is capable of executing jobs.

        Args:
            route_service (RouteService):
                Service use to communicate with other nodes.
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
            datasources (list[dict[str, Any]]):
                List of maps to available datasources. Can be obtained from node configuration.
        """
        self.route_service: RouteService = route_service

        self.component_id: str = component_id
        self.artifact_id: str = artifact_id
        self.job_id: str = job_id

        self.scheduler_id: str = scheduler_id
        self.scheduler_url: str = scheduler_url
        self.scheduler_public_key: str = scheduler_public_key

        self.data = DataSourceStorage([DataSourceConfiguration(**ds) for ds in datasources])

    def __repr__(self) -> str:
        return f"Job={self.job_id} artifact={self.artifact_id}"

    def scheduler(self) -> RouteService:
        self.route_service.change_route(
            self.scheduler_id,
            self.scheduler_public_key,
            self.scheduler_url,
        )
        return self.route_service

    def resource(
        self,
        target_id: str,
        target_url: str,
        target_public_key: str,
    ) -> RouteService:
        self.route_service.change_route(
            target_id,
            target_url,
            target_public_key,
        )
        return self.route_service

    def product(
        self,
        target_id: str,
        target_url: str,
        target_public_key: str,
        proxy_public_key: str | None,
    ) -> RouteService:
        self.route_service.change_route(
            target_id,
            target_public_key,
            target_url,
            proxy_public_key,
        )
        return self.route_service

    def run(self):
        artifact_id: str = self.artifact_id
        job_id: str = self.job_id

        LOGGER.info(f"JOB job={job_id}: received task")

        try:
            task: Task = self.scheduler().get_task_data(artifact_id, job_id)

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
                    LOGGER.info(
                        f"JOB job={job_id}: obtaining resource from node={resource.component_id} url={resource.component_url}"
                    )

                    # path to resource saved locally
                    res_path: Path = self.resource(
                        resource.component_id,
                        resource.component_url,
                        resource.component_public_key,
                    ).get_resource(
                        resource.component_id,
                        resource.artifact_id,
                        resource.job_id,
                        resource.resource_id,
                        resource.iteration,
                    )

                es.add_resource(resource.resource_id, res_path)

            # apply work from step
            LOGGER.info(f"JOB job={job_id}: starting execution")

            es.run()

            if not es.has_product():
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
                    f"proxy={next_node.use_scheduler_as_proxy} "
                    f"is_local={is_local}"
                )
                if next_node.use_scheduler_as_proxy:
                    remote_url = self.scheduler_url
                    remote_key = self.scheduler_public_key
                else:
                    remote_url = next_node.target_url
                    remote_key = None

                path = None if is_local else es.env.product_path()

                self.product(
                    next_node.target_id,
                    remote_url,
                    next_node.target_public_key,
                    remote_key,
                ).post_resource(artifact_id, job_id, task.produced_resource_id, path)

            self.scheduler().post_done(artifact_id, job_id)

            LOGGER.info(f"JOB job={job_id}: execution completed with success")

        except Exception as e:
            LOGGER.error(f"JOB job={job_id}: execution failed")
            LOGGER.exception(e)

            self.scheduler().post_error(
                job_id,
                TaskError(
                    job_id=job_id,
                    message=f"{e}",
                    stack_trace="".join(traceback.TracebackException.from_exception(e).format()),
                ),
            )


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
        scheduler_is_local: bool,
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

        self.task_executor = TaskExecutor(
            route_service,
            component_id,
            artifact_id,
            job_id,
            scheduler_id,
            scheduler_url,
            scheduler_public_key,
            datasources,
        )

    def run(self) -> None:
        self.task_executor.run()
