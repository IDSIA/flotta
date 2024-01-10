from ferdelance.database.repositories import ArtifactRepository, JobRepository, Repository, ResourceRepository
from ferdelance.schemas.database import Resource
from ferdelance.schemas.resources import ResourceIdentifier

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import NoResultFound


class ResourceManagementService(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.jr: JobRepository = JobRepository(session)
        self.rr: ResourceRepository = ResourceRepository(session)

    async def create_resource_external(self, producer_id: str) -> Resource:
        return await self.rr.create_resource_external(producer_id)

    async def create_resource(self, job_id: str, artifact_id: str, producer_id: str, iteration: int) -> Resource:
        return await self.rr.create_resource(job_id, artifact_id, producer_id, iteration)

    async def store_resource(self, job_id: str, producer_id: str) -> Resource:
        # simple check that the job exists
        job = await self.jr.get_by_id(job_id)

        if job.component_id != producer_id:
            raise ValueError("Job's component and producer_id are not equals")

        # simple check that the artifact exists
        await self.ar.get_artifact(job.artifact_id)

        return await self.rr.get_by_job_id(job_id)

    async def load_resource(self, res: ResourceIdentifier) -> Resource:
        """
        :raise:
            NoResultFound if there is no resource on the disk.
        """

        if res.resource_id is not None:
            return await self.rr.get_by_id(res.resource_id)

        if res.artifact_id is not None and res.producer_id is not None and res.iteration is not None:
            return await self.rr.get_by_producer_id(res.artifact_id, res.producer_id, res.iteration)

        raise NoResultFound()
