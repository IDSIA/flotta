from typing import Any

from ferdelance.config import config_manager, Configuration
from ferdelance.const import TYPE_CLIENT
from ferdelance.database.repositories import (
    AsyncSession,
    ArtifactRepository,
    ComponentRepository,
    JobRepository,
    Repository,
)
from ferdelance.logging import get_logger
from ferdelance.node.services.jobs import JobManagementService
from ferdelance.schemas.components import Component
from ferdelance.security.exchange import Exchange
from ferdelance.shared.status import ArtifactJobStatus, JobStatus
from ferdelance.tasks.backends import get_jobs_backend
from ferdelance.tasks.tasks import Task

import httpx


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

        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.jr: JobRepository = JobRepository(session)
        self.cr: ComponentRepository = ComponentRepository(self.session)

        self.self_component: Component = self_component
        self.private_key: str = private_key
        self.public_key: str = public_key

        self.config: Configuration = config_manager.get()

        self.local_datasources: list[dict[str, Any]] = [ds.model_dump() for ds in config_manager.get_data().ds_configs]

    async def check(self, artifact_id: str) -> None:
        """Checks if there are scheduled tasks that need to be started, locally or remotely.

        Args:
            artifact_id (str):
                Id of the artifact we are working on.
        """
        LOGGER.info(f"component={self.self_component.id}: checking jobs to launch for artifact={artifact_id}")

        artifact = await self.ar.get_artifact(artifact_id)

        if artifact.status in (ArtifactJobStatus.COMPLETED, ArtifactJobStatus.ERROR):
            LOGGER.info(f"component={self.self_component.id}: artifact={artifact_id} already completed")
            return

        if artifact.status != ArtifactJobStatus.RUNNING:
            LOGGER.warning(
                f"component={self.self_component.id}: artifact={artifact_id} status={artifact.status} not in RUNNING state"
            )
            return

        scheduled_jobs = await self.jr.list_scheduled_jobs_for_artifact(artifact_id)

        LOGGER.info(f"component={self.self_component.id}: found {len(scheduled_jobs)} job(s) in SCHEDULED state")

        for job in scheduled_jobs:
            if job.status != JobStatus.SCHEDULED:
                LOGGER.warning(
                    f"component={self.self_component.id}: job={job.id} "
                    f"status={job.status} is not in SCHEDULED status anymore"
                )
                continue

            if job.component_id == self.self_component.id:
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
            component_id,
            self.config.url_localhost(),
            public_key,
            self.local_datasources,
        )

    async def start_task(self, task: Task, scheduler_id: str) -> None:
        scheduler_node = await self.cr.get_by_id(scheduler_id)

        get_jobs_backend().start_exec(
            task.artifact_id,
            task.job_id,
            self.self_component.id,
            self.private_key,
            scheduler_node.id,
            scheduler_node.url,
            scheduler_node.public_key,
            self.local_datasources,
        )

    async def start_remote(
        self,
        job_id: str,
        remote_component_id: str,
        # this node private key used for communication
        private_key: str,
    ) -> None:
        remote = await self.cr.get_by_id(remote_component_id)

        if remote.type_name == TYPE_CLIENT:
            LOGGER.info(
                f"component={self.self_component.id}: job={job_id} start with remote={remote.id} postponed to heartbeat"
            )
            return

        LOGGER.info(f"component={self.self_component.id}: job={job_id} will start on remote={remote.id}")

        jms: JobManagementService = JobManagementService(self.session, self.self_component)

        task: Task = await jms.get_task_by_job_id(job_id)

        exc: Exchange = Exchange(self.self_component.id, private_key)
        exc.set_remote_key(remote.id, remote.public_key)

        headers, payload = exc.create(task.model_dump_json())

        res = httpx.post(
            f"{remote.url.rstrip('/')}/task/",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()
