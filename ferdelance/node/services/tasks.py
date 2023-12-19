from typing import Any

from ferdelance.config import config_manager
from ferdelance.const import TYPE_CLIENT
from ferdelance.database.repositories import AsyncSession, Repository, ComponentRepository
from ferdelance.database.repositories.jobs import JobRepository
from ferdelance.logging import get_logger
from ferdelance.node.services.jobs import JobManagementService
from ferdelance.schemas.components import Component
from ferdelance.shared.exchange import Exchange
from ferdelance.tasks.backends import get_jobs_backend
from ferdelance.tasks.tasks import Task

import requests


LOGGER = get_logger(__name__)


class TaskManagementService(Repository):
    def __init__(
        self,
        session: AsyncSession,
        self_component: Component,
        private_key: str,
        public_key: str,
    ) -> None:
        super().__init__(session)

        self.jr: JobRepository = JobRepository(session)
        self.cr: ComponentRepository = ComponentRepository(self.session)

        self.self_component: Component = self_component
        self.private_key: str = private_key
        self.public_key: str = public_key

        self.local_datasources: list[dict[str, Any]] = [ds.dict() for ds in config_manager.data.ds_configs]

    async def check(self, artifact_id: str) -> None:
        """Checks if there are scheduled tasks that need to be started, locally or remotely.

        Args:
            artifact_id (str):
                Id of the artifact we are working on.
        """
        scheduled_jobs = await self.jr.list_scheduled_jobs_for_artifact(artifact_id)

        LOGGER.info(f"component={self.self_component.id}: found {len(scheduled_jobs)} job(s) in SCHEDULED state")

        for job in scheduled_jobs:
            if job.component_id == self.self_component:
                await self.start_locally(
                    job.artifact_id,
                    job.id,
                    job.component_id,
                    self.private_key,
                    self.public_key,
                )
            else:
                await self.start_remote(
                    job.id,
                    job.component_id,
                    self.private_key,
                )

    async def start_locally(
        self,
        artifact_id: str,
        job_id: str,
        component_id: str,
        private_key: str,
        public_key: str,
    ) -> None:
        """Starts a task on the same node where is located the scheduler.

        Args:
            artifact_id (str):
                Id of the artifact.
            job_id (str):
                Id of the task that will be executed.
            component_id (str):
                Id of the node (self_component).
            private_key (str):
                Private key of the node (self_component).
            public_key (str):
                Public key of the node (self_component).
        """
        LOGGER.info(f"component={self.self_component.id}: job={job_id} will start in local")

        get_jobs_backend().start_exec(
            artifact_id,
            job_id,
            component_id,
            private_key,
            "http://localhost/",
            public_key,
            self.local_datasources,
            True,
        )

    async def start_task(self, task: Task, scheduler_id: str) -> None:
        scheduler_node = await self.cr.get_by_id(scheduler_id)

        get_jobs_backend().start_exec(
            task.artifact_id,
            task.job_id,
            self.self_component.id,
            self.private_key,
            scheduler_node.url,
            scheduler_node.public_key,
            self.local_datasources,
            False,
        )

    async def start_remote(
        self,
        job_id: str,
        component_id: str,
        # this node private key used for communication
        private_key: str,
    ) -> None:
        remote = await self.cr.get_by_id(component_id)

        if remote.type_name == TYPE_CLIENT:
            LOGGER.info(
                f"component={self.self_component.id}: job={job_id} start with remote={remote.id} postponed to heartbeat"
            )
            return

        LOGGER.info(f"component={self.self_component.id}: job={job_id} will start on remote={remote.id}")

        jms: JobManagementService = JobManagementService(self.session, self.self_component)

        task: Task = await jms.get_task_by_job_id(job_id)

        exc: Exchange = Exchange()
        exc.set_private_key(private_key)
        exc.set_remote_key(remote.public_key)

        headers, payload = exc.create(self.self_component.id, task.json())

        res = requests.post(
            f"{remote.url}/task",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()
