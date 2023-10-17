from ferdelance.config import config_manager
from ferdelance.schemas.database import Resource
from ferdelance.database.tables import Resource as ResourceDB
from ferdelance.database.repositories.core import AsyncSession, Repository

from sqlalchemy import select
from uuid import uuid4


def view(resource: ResourceDB) -> Resource:
    return Resource(
        id=resource.id,
        job_id=resource.job_id,
        artifact_id=resource.artifact_id,
        component_id=resource.component_id,
        path=resource.path,
        creation_time=resource.creation_time,
        is_model=resource.is_model,
        is_estimation=resource.is_estimation,
        is_aggregation=resource.is_aggregation,
        iteration=resource.iteration,
    )


class ResourceRepository(Repository):
    """A repository for the resource of training and estimation tasks. This object
    can collect anything produced by clients (models, estimators) and workers
    (aggregated models).
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_resource(
        self,
        job_id: str,
        artifact_id: str,
        producer_id: str,
        iteration: int,
        is_estimation: bool = False,
        is_model: bool = False,
        is_aggregation: bool = False,
        is_error: bool = False,
    ) -> Resource:
        """Creates an entry in the database for the resource produced by a client or a worker,
        identified with producer_id, and by setting the type of resource as a specified by the flags.

        Args:
            artifact_id (str):
                The resource will be produced and associated to this artifact_id
            producer_id (str):
                The component_id of whom has produced the resource.
            is_estimation (bool, optional):
                Set to true when the resource is an estimation.
                Defaults to False.
            is_model (bool, optional):
                Set to true when the resource is a model.
                Defaults to False.
            is_aggregation (bool, optional):
                Set to true when the resource is an aggregation.
                Defaults to False.
            is_error (bool, optional):
                Set to true when the resource is an error.
                Defaults to False.

        Returns:
            Resource:
                An handler to the recorded resource in the database. This handler can be
                used to obtain the output path and save the resource to disk.
        """

        resource_id: str = str(uuid4())

        out_path = config_manager.get().store_resource(
            artifact_id,
            job_id,
            iteration,
            is_error,
            is_aggregation,
            is_model,
            is_estimation,
        )

        resource_db = ResourceDB(
            id=resource_id,
            path=out_path,
            job_id=job_id,
            artifact_id=artifact_id,
            component_id=producer_id,
            is_estimation=is_estimation,
            is_model=is_model,
            is_aggregation=is_aggregation,
            is_error=is_error,
            iteration=iteration,
        )

        self.session.add(resource_db)
        await self.session.commit()
        await self.session.refresh(resource_db)

        return view(resource_db)

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

    async def get_model_by_id(self, resource_id: str) -> Resource:
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
            select(ResourceDB).where(
                ResourceDB.id == resource_id,
                ResourceDB.is_model,
            )
        )
        return view(res.one())

    async def get_estimator_by_id(self, resource_id: str) -> Resource:
        """Get the resource, considered an estimation, given its resource_id.

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
                ResourceDB.is_estimation,
            )
        )
        return view(res.one())

    async def list_resources_by_artifact_id(self, artifact_id: str, iteration: int) -> list[Resource]:
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
        res = await self.session.scalars(
            select(ResourceDB).where(
                ResourceDB.artifact_id == artifact_id,
                ResourceDB.iteration == iteration,
            )
        )
        resource_list = [view(m) for m in res.all()]
        return resource_list

    async def list_models_by_artifact_id(self, artifact_id: str) -> list[Resource]:
        """Get a list of models associated with the given artifact_id. This
        returns all kind of resources, both partial and aggregated.

        Args:
            artifact_id (str):
                Id of the artifact to search for.

        Returns:
            Resource:
                A list of all the models associated with the given artifact_id.
                Note that his list can also be empty.
        """
        res = await self.session.scalars(
            select(ResourceDB).where(
                ResourceDB.artifact_id == artifact_id,
                ResourceDB.is_model == True,  # noqa: E712
            )
        )
        resource_list = [view(m) for m in res.all()]
        return resource_list

    async def list_models(self) -> list[Resource]:
        """Returns a list of all the resources that are models, partial and aggregated,
        stored in the database.

        Returns:
            list[Resource]:
                A list of resources. Note that this list can be empty.
        """
        res = await self.session.scalars(
            select(ResourceDB).where(
                ResourceDB.is_model,
            )
        )
        resource_list = [view(r) for r in res.all()]
        return resource_list

    async def list_estimations(self) -> list[Resource]:
        """Returns al list of all the resources that are estimations, partial and
        aggregated, stored in the database.

        Returns:
            list[Resource]:
                A list of resources. Note that this list can be empty.
        """
        res = await self.session.scalars(
            select(ResourceDB).where(
                ResourceDB.is_estimation,
            )
        )
        resource_list = [view(r) for r in res.all()]
        return resource_list

    async def get_aggregated_resource(self, artifact_id: str) -> Resource:
        """Get the resource, considered an aggregated model or estimation, given
        the artifact id.

        Note that for each artifact, only one aggregated resource can exists.

        Args:
            artifact_id (str):
                Id of the artifact to get.

        Raises:
            NoResultFound:
                If the resource does not exists.

        Returns:
            Resource:
                The handler to the resource, if one is found.
        """
        res = await self.session.scalars(
            select(ResourceDB).where(
                ResourceDB.artifact_id == artifact_id,
                ResourceDB.is_aggregation == True,  # noqa: E712
            )
        )
        return view(res.one())

    async def get_partial_resource(self, artifact_id: str, client_id: str, iteration: int) -> Resource:
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
            select(ResourceDB).where(
                ResourceDB.artifact_id == artifact_id,
                ResourceDB.component_id == client_id,
                ResourceDB.is_aggregation == False,  # noqa: E712
                ResourceDB.iteration == iteration,
            )
        )
        return view(res.one())
