from ferdelance.database import AsyncSession
from ferdelance.database.repositories import ResultRepository
from ferdelance.jobs import job_manager, JobManagementService
from ferdelance.schemas.artifacts import ArtifactStatus, Artifact
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import Result
from ferdelance.schemas.errors import ErrorArtifact
from ferdelance.schemas.worker import WorkerTask

import logging
import os

LOGGER = logging.getLogger(__name__)


class WorkerService:
    def __init__(self, session: AsyncSession, worker: Component) -> None:
        self.session: AsyncSession = session
        self.worker: Component = worker
        self.jms: JobManagementService = job_manager(self.session)

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        """
        :raise:
            ValueError if the artifact already exists.
        """
        status = await self.jms.submit_artifact(artifact)

        LOGGER.info(f"worker_id={self.worker.component_id}: submitted artifact got artifact_id={status.artifact_id}")

        return status

    async def get_task(self, job_id: str) -> WorkerTask:
        task: WorkerTask = await self.jms.worker_task_start(job_id, self.worker.component_id)

        return task

    async def result(self, job_id: str) -> Result:
        result = await self.jms.client_result_create(job_id, self.worker.component_id)

        return result

    async def get_result(self, result_id: str) -> Result:
        rr: ResultRepository = ResultRepository(self.session)

        result: Result = await rr.get_by_id(result_id)

        return result

    async def completed(self, job_id: str) -> None:
        """Aggregation completed."""
        await self.jms.aggregation_completed(job_id)

    async def failed(self, error: ErrorArtifact) -> Result:
        """Aggregation failed."""
        result = await self.jms.worker_error(error.artifact_id, self.worker.component_id)

        await self.error(error.artifact_id, error.message)

        return result

    async def error(self, job_id: str, msg: str) -> None:
        """Worker did an error."""
        await self.jms.aggregation_error(job_id, msg)
