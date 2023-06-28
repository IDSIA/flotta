from ferdelance.database import AsyncSession
from ferdelance.database.repositories import (
    ResultRepository,
    JobRepository,
    ArtifactRepository,
)
from ferdelance.server.services import JobManagementService
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import Result, ServerArtifact
from ferdelance.schemas.errors import WorkerAggregationJobError
from ferdelance.schemas.worker import WorkerTask
from ferdelance.shared.status import ArtifactJobStatus

from sqlalchemy.exc import NoResultFound

import aiofiles
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


class WorkerService:
    def __init__(self, session: AsyncSession, component: Component) -> None:
        self.session: AsyncSession = session
        self.component: Component = component
        self.jms: JobManagementService = JobManagementService(self.session)

    async def get_task(self, job_id: str) -> WorkerTask:
        jr: JobRepository = JobRepository(self.session)
        ar: ArtifactRepository = ArtifactRepository(self.session)
        rr: ResultRepository = ResultRepository(self.session)

        try:
            job = await jr.get_by_id(job_id)
            artifact_id = job.artifact_id

            artifact_db: ServerArtifact = await ar.get_artifact(artifact_id)

            if ArtifactJobStatus[artifact_db.status] != ArtifactJobStatus.AGGREGATING:
                raise ValueError(f"Wrong status for job_id={job_id}")

            async with aiofiles.open(artifact_db.path, "r") as f:
                data = await f.read()
                artifact = Artifact(**json.loads(data))

            await jr.start_execution(job)

            results: list[Result] = await rr.list_results_by_artifact_id(artifact_id, artifact_db.iteration)

            return WorkerTask(
                artifact=artifact,
                job_id=job_id,
                result_ids=[r.id for r in results],
            )

        except NoResultFound as _:
            raise ValueError(f"worker_id={self.component.id}: task with job_id={job_id} does not exists")

    async def result(self, job_id: str) -> Result:
        LOGGER.info(f"worker_id={self.component.id}: creating aggregated result for job_id={job_id}")
        try:
            return await self.jms.create_result(job_id, self.component.id, True)

        except NoResultFound as _:
            raise ValueError(f"worker_id={self.component.id}: task with job_id={job_id} does not exists")

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

    async def aggregation_completed(self, job_id: str) -> Result:
        """Aggregation completed."""
        LOGGER.info(f"job_id={job_id}: aggregation completed")

        await self.jms.aggregation_completed(job_id, self.component.id)

        return await self.jms.create_result(job_id, self.component.id, True)

    async def check_next_iteration(self, job_id: str) -> None:
        await self.jms.check_for_iteration(job_id)

    async def aggregation_failed(self, error: WorkerAggregationJobError) -> Result:
        """Aggregation failed, and worker did an error."""
        LOGGER.info(f"job_id={error.job_id}: aggregation failed")

        await self.jms.aggregation_failed(error, self.component.id)

        return await self.jms.create_result(error.job_id, self.component.id, True, True)
