import uuid

from sqlalchemy import select, and_
from sqlalchemy.exc import NoReferenceError

from ferdelance.database.schemas import Project
from ferdelance.database.services.core import AsyncSession, DBSessionService
from ferdelance.database.services.datasource import DataSourceService
from ferdelance.database.services.tokens import TokenService
from ferdelance.database.tables import DataSource, Project as ProjectDB
from ferdelance.shared.artifacts import Metadata


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
        self.dss: DataSourceService = DataSourceService(session)

    async def create(self, name: str = "", token: str | None = None) -> str:

        if token is None:
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

            ds: DataSource = await self.session.scalar(
                select(DataSource).where(DataSource.datasource_id == datasource_id)
            )
            p: ProjectDB = await self.session.scalar(select(ProjectDB).where(ProjectDB.project_id == project_id))

            p.datasources.append(ds)

            self.session.add(p)
            await self.session.commit()

        except NoReferenceError:
            raise ValueError()

    async def add_datasources_from_metadata(self, metadata: Metadata) -> None:
        for mdds in metadata.datasources:

            ds: DataSource = await self.session.scalar(
                select(DataSource).where(DataSource.datasource_id == mdds.datasource_id)
            )

            if not mdds.tokens:
                continue

            res = await self.session.scalars(select(ProjectDB.project_id).filter(ProjectDB.token.in_(mdds.tokens)))
            project_ids: list[str] = list(res.all())

            if not project_ids:
                # TODO: should this be an error?
                continue

            for project_id in project_ids:
                p: ProjectDB = await self.session.scalar(select(ProjectDB).where(ProjectDB.project_id == project_id))
                p.datasources.append(ds)
                self.session.add(p)

            await self.session.commit()

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

    async def get_by_name_and_token(self, name: str, token: str) -> Project:
        """Can raise NoResultsException."""
        query = await self.session.execute(
            select(ProjectDB).where(and_(ProjectDB.name == name, ProjectDB.token == token))
        )
        res: ProjectDB = query.scalar_one()
        return view(res)
