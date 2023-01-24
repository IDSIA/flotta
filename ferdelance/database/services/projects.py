import uuid

from sqlalchemy import select
from sqlalchemy.exc import NoReferenceError

from ferdelance.database.schemas import Project
from ferdelance.database.services.core import AsyncSession, DBSessionService
from ferdelance.database.services.tokens import TokenService
from ferdelance.database.tables import Project as ProjectDB
from ferdelance.database.tables import ProjectDataSource as ProjectDataSourceDB


def view(project: ProjectDB) -> Project:
    return Project(
        project_id=project.project_id,
        name=project.name,
        creation_time=project.creation_time,
        token=project.token,
        valid=project.valid,
        active=project.active,
    )


class ProjectService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.ts: TokenService = TokenService(session)

    async def create(self, name: str = "") -> str:

        token = await self.ts.project_token(name)

        project = ProjectDB(
            project_id=str(uuid.uuid4()),
            name=name,
            token=token,
        )

        self.session.add(project)
        await self.session.commit()

        return token

    async def add_datasource(self, datasource_id: str, project_id: str) -> None:
        """Can raise ValueError."""
        try:
            pds = ProjectDataSourceDB(
                project_id=project_id,
                datasource_id=datasource_id,
            )

            self.session.add(pds)
            await self.session.commit()

        except NoReferenceError:
            raise ValueError()

    async def get_project_list(self) -> list[Project]:
        res = await self.session.execute(select(ProjectDB))
        project_db_list = res.scalars().all()
        return [view(p) for p in project_db_list]

    async def get_by_id(self, project_id: str) -> Project:
        """Can raise NoResultsException."""
        query = await self.session.execute(select(ProjectDB).where(ProjectDB.project_id == project_id))
        res: ProjectDB = query.scalar_one()
        return view(res)

    async def get_by_token(self, token: str) -> Project:
        """Can raise NoResultsException."""
        query = await self.session.execute(select(ProjectDB).where(ProjectDB.token == token))
        res: ProjectDB = query.scalar_one()
        return view(res)
