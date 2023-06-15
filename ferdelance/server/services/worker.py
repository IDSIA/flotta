from ferdelance.database import AsyncSession
from ferdelance.database.repositories import ResultRepository
from ferdelance.jobs import job_manager, JobManagementService
from ferdelance.schemas.artifacts import ArtifactStatus, Artifact
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import Result
from ferdelance.schemas.errors import ErrorArtifact
from ferdelance.schemas.worker import WorkerTask

from sqlalchemy.exc import NoResultFound

import logging
import os

LOGGER = logging.getLogger(__name__)


class WorkerService:
    def __init__(self, session: AsyncSession, component: Component) -> None:
        self.session: AsyncSession = session
        self.component: Component = component
        self.jms: JobManagementService = job_manager(self.session)

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        """
        :raise:
            ValueError if the artifact already exists.
        """
        status = await self.jms.submit_artifact(artifact)

        LOGGER.info(f"worker_id={self.component.id}: submitted artifact got artifact_id={status.id}")

        return status

    async def get_task(self, job_id: str) -> WorkerTask:
        task: WorkerTask = await self.jms.worker_task_start(job_id, self.component.id)

        return task

    async def result(self, job_id: str) -> Result:
        result = await self.jms.client_result_create(job_id, self.component.id)

        return result

    async def get_result(self, result_id: str) -> Result:
        """
        :raise:
            NoResultFound if there is no result on the disk.
        """
        rr: ResultRepository = ResultRepository(self.session)

        result: Result = await rr.get_by_id(result_id)

        if not os.path.exists(result.path):
            raise NoResultFound()

        return result

    async def completed(self, job_id: str) -> None:
        """Aggregation completed."""
        await self.jms.aggregation_completed(job_id)

    async def failed(self, error: ErrorArtifact) -> Result:
        """Aggregation failed, and worker did an error."""
        result = await self.jms.worker_error(error.artifact_id, self.component.id)

        await self.error(error.artifact_id, error.message)

        return result

    async def error(self, job_id: str, msg: str) -> None:
        """Worker did an error."""
        await self.jms.aggregation_error(job_id, msg)
