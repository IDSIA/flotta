"""Implementation of the CLI features regarding models"""

from flotta.cli.visualization import show_many, show_one
from flotta.database import DataBase
from flotta.database.repositories import ResourceRepository
from flotta.schemas.database import Resource

from sqlalchemy.exc import NoResultFound


async def list_resource(artifact_id: str | None = None) -> list[Resource]:
    """Print model list, with or without filters on ARTIFACT_ID, MODEL_ID"""
    # TODO depending on 1:1, 1:n relations with artifacts arguments change or disappear

    db = DataBase()
    async with db.async_session() as session:
        resource_repository = ResourceRepository(session)

        if artifact_id is not None:
            resources: list[Resource] = await resource_repository.list_resources_by_artifact_id(artifact_id, False)
        else:
            resources: list[Resource] = await resource_repository.list_resources()

        show_many(resources)

        return resources


async def describe_resource(resource_id: str | None = None) -> Resource | None:
    """Describe single model by printing its db record.

    Args:
        resource_id (str): Unique ID of the resource

    Raises:
        ValueError: if no model id is provided

    Returns:
        ServerTask: the Task object produced by a client
    """

    if resource_id is None:
        raise ValueError("Provide a Task ID")

    db = DataBase()
    async with db.async_session() as session:
        task_repository = ResourceRepository(session)

        try:
            resource: Resource = await task_repository.get_by_id(resource_id)
            show_one(resource)

            return resource

        except NoResultFound as _:
            print(f"No model found with id {resource_id}")
