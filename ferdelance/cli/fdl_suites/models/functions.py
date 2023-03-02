"""Implementation of the CLI features regarding models"""

from sqlalchemy.exc import NoResultFound

from ferdelance.database import DataBase
from ferdelance.schemas.database import ServerTask
from ferdelance.database.repositories import TaskRepository
from ferdelance.cli.visualization import show_many, show_one


async def list_models(artifact_id: str | None = None) -> list[ServerTask]:
    """Print model list, with or without filters on ARTIFACT_ID, MODEL_ID"""
    # TODO depending on 1:1, 1:n relations with artifacts arguments change or disappear

    db = DataBase()
    async with db.async_session() as session:

        task_repository = TaskRepository(session)

        if artifact_id is not None:
            models: list[ServerTask] = await task_repository.get_models_by_artifact_id(artifact_id)
        else:
            models: list[ServerTask] = await task_repository.get_model_list()

        show_many(models)

        return models


async def describe_model(task_id: str | None = None) -> ServerTask | None:
    """Describe single model by printing its db record.

    Args:
        model_id (str): Unique ID of the model

    Raises:
        ValueError: if no model id is provided

    Returns:
        ServerTask: the Task object produced by a client
    """

    if task_id is None:
        raise ValueError("Provide a Task ID")

    db = DataBase()
    async with db.async_session() as session:

        task_repository = TaskRepository(session)

        try:
            model: ServerTask = await task_repository.get_model_by_id(task_id)
            show_one(model)

            return model

        except NoResultFound as e:
            print(f"No model found with id {task_id}")
