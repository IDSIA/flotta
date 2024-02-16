from ferdelance.config import config_manager
from ferdelance.database.tables import Job as JobDB, Resource as ResourceDB
from ferdelance.database.repositories.core import AsyncSession, Repository
from ferdelance.schemas.database import Resource

from datetime import datetime
from sqlalchemy import select
from pathlib import Path
from uuid import uuid4


def view(resource: ResourceDB) -> Resource:
    return Resource(
        id=resource.id,
        component_id=resource.component_id,
        path=Path(resource.path),
        creation_time=resource.creation_time,
        is_external=resource.is_external,
        is_error=resource.is_error,
        is_ready=resource.is_ready,
        encrypted_for=resource.encrypted_for,
    )


class ResourceRepository(Repository):
    """A repository for the resource of training and estimation tasks. This object
    can collect anything produced by clients (models, estimators) and workers
    (aggregated models).
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_resource_external(
        self,
        producer_id: str,
    ) -> Resource:
        resource_id: str = str(uuid4())

        out_path = config_manager.get().storage_resource(resource_id)

        resource_db = ResourceDB(
            id=resource_id,
            path=str(out_path),
            is_external=True,
            component_id=producer_id,
        )

        self.session.add(resource_db)
        await self.session.commit()
        await self.session.refresh(resource_db)

        return view(resource_db)

    async def create_resource(
        self,
        job_id: str,
        artifact_id: str,
        producer_id: str,
        iteration: int,
    ) -> Resource:
        """Creates an entry in the database for the resource produced by a client or a worker,
        identified with producer_id, and by setting the type of resource as a specified by the flags.

        Args:
            artifact_id (str):
                The resource will be produced and associated to this artifact_id
            producer_id (str):
                The component_id of whom has produced the resource.
            is_error (bool, optional):
                Set to true when the resource is an error.
                Defaults to False.

        Returns:
            Resource:
                An handler to the recorded resource in the database. This handler can be
                used to obtain the output path and save the resource to disk.
        """

        resource_id: str = str(uuid4())

        out_path = config_manager.get().storage_job(artifact_id, job_id, iteration) / f"{resource_id}.pkl"

        resource_db = ResourceDB(
            id=resource_id,
            path=str(out_path),
            component_id=producer_id,
        )

        self.session.add(resource_db)
        await self.session.commit()
        await self.session.refresh(resource_db)

        return view(resource_db)

    async def mark_as_done(self, job_id: str) -> Resource:
        res = await self.session.scalars(
            select(ResourceDB)
            .join(JobDB)
            .where(
                JobDB.id == job_id,
            )
        )

        resource = res.one()

        resource.is_ready = True
        resource.creation_time = datetime.now()
        await self.session.commit()
        await self.session.refresh(resource)

        return view(resource)

    async def mark_as_error(self, job_id: str) -> Resource:
        res = await self.session.scalars(
            select(ResourceDB)
            .join(JobDB)
            .where(
                JobDB.id == job_id,
            )
        )

        resource = res.one()

        resource.is_error = True
        await self.session.commit()
        await self.session.refresh(resource)

        return view(resource)

    async def set_encrypted_for(self, resource_id: str, component_id: str) -> Resource:
        res = await self.session.scalars(
            select(ResourceDB).where(
                ResourceDB.id == resource_id,
            )
        )

        resource = res.one()

        resource.encrypted_for = component_id

        await self.session.commit()
        await self.session.refresh(resource)

        return view(resource)

    async def get_by_id(self, resource_id: str) -> Resource:
        """Get the resource given its resource_id.

        Args:
            resource_id (str):
                Id of the resource to get.

        Raises:
            NoResultFound:
                If the resource does not exists

        Returns:
            Resource:
                The handler to the resource, if one is found.
        """
        res = await self.session.scalars(
            select(ResourceDB).where(
                ResourceDB.id == resource_id,
            )
        )

        return view(res.one())

    async def get_by_job_id(self, job_id: str) -> Resource:
        """Get the resource, considered a model, given its resource_id.

        Args:
            resource_id (str):
                Id of the resource to get.

        Raises:
            NoResultFound:
                If the resource does not exists

        Returns:
            Resource:
                The handler to the resource, if one is found.
        """
        res = await self.session.scalars(
            select(ResourceDB)
            .join(JobDB)
            .where(
                JobDB.id == job_id,
            )
        )
        return view(res.one())

    async def list_resources_by_artifact_id(
        self, artifact_id: str, only_complete: bool, iteration: int = -1
    ) -> list[Resource]:
        """Get a list of resources associated with the given artifact_id. This
        returns all kind of resources, models and estimations, aggregated or not.

        Args:
            artifact_id (str):
                Id of the artifact to search for.

        Returns:
            Resource:
                A list of all the resources associated with the given artifact_id.
                Note that his list can also be empty.
        """
        conditions = [
            JobDB.artifact_id == artifact_id,
        ]
        if iteration > -1:
            conditions.append(JobDB.iteration == iteration)

        if only_complete:
            conditions.append(ResourceDB.is_ready == only_complete)

        res = await self.session.scalars(
            select(ResourceDB)
            .join(JobDB)
            .where(
                *conditions,
            )
        )
        return [view(m) for m in res.all()]

    async def list_resources(self) -> list[Resource]:
        """Get a list of resources associated with the given artifact_id. This
        returns all kind of resources, models and estimations, aggregated or not.

        Args:
            artifact_id (str):
                Id of the artifact to search for.

        Returns:
            Resource:
                A list of all the resources associated with the given artifact_id.
                Note that his list can also be empty.
        """
        res = await self.session.scalars(select(ResourceDB))
        return [view(m) for m in res.all()]

    async def get_by_producer_id(self, artifact_id: str, producer_id: str, iteration: int) -> Resource:
        """Get the resource, considered as a partial model or estimation, given
        the artifact_id it belongs to and the client_id that produced the resource.

        Note that for each pair artifact_id - client_id, only on aggregated
        resource can exists.

        Args:
            artifact_id (str):
                Id of the artifact to get.
            client_id (str):
                Id of the client that produced the partial model.

        Raises:
            NoResultFound:
                If the resource does not exists.

        Returns:
            Resource:
                The handler to the resource, if one is found.
        """
        res = await self.session.scalars(
            select(ResourceDB)
            .join(JobDB)
            .where(
                ResourceDB.component_id == producer_id,
                JobDB.artifact_id == artifact_id,
                JobDB.iteration == iteration,
            )
        )
        return view(res.one())
