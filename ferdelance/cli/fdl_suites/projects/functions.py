from ferdelance.cli.visualization import show_many, show_one, show_string
from ferdelance.database import DataBase
from ferdelance.schemas.project import BaseProject as ProjectView
from ferdelance.database.services import ProjectService

from sqlalchemy.exc import NoResultFound


async def list_projects() -> list[ProjectView]:
    db = DataBase()
    async with db.async_session() as session:
        project_service: ProjectService = ProjectService(session)
        projects: list[ProjectView] = await project_service.get_project_list()
        show_many(projects)
        return projects


async def create_project(name: str) -> str:
    db = DataBase()
    async with db.async_session() as session:
        project_service: ProjectService = ProjectService(session)
        project_token: str = await project_service.create(name=name)
        show_string(project_token)
        return project_token


async def describe_project(project_id: str = None, token: str = None) -> ProjectView | None:

    if project_id is not None and token is not None or project_id is None and token is None:
        raise ValueError("Specify either the project_id or the token of the project")

    db = DataBase()
    async with db.async_session() as session:
        project_service: ProjectService = ProjectService(session)

        try:

            if project_id is not None:
                project: ProjectView = await project_service.get_by_id(project_id=project_id)
            else:
                project: ProjectView = await project_service.get_by_token(token=token)

            show_one(project)

            return project

        except NoResultFound as _:
            print(f"No project found with id id or token {project_id or token}")
