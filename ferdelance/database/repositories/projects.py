from ferdelance.database.repositories.core import AsyncSession, Repository
from ferdelance.database.repositories.tokens import TokenRepository
from ferdelance.database.repositories.datasource import DataSourceRepository
from ferdelance.database.tables import (
    DataSource as DataSourceDB,
    Project as ProjectDB,
)
from ferdelance.schemas.datasources import DataSource
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.project import (
    Project,
    BaseProject,
    AggregatedDataSource,
)

from sqlalchemy import select
from sqlalchemy.exc import NoReferenceError
from sqlalchemy.orm import selectinload

import uuid


def simpleView(project: ProjectDB) -> BaseProject:
    return BaseProject(
        project_id=project.project_id,
        token=project.token,
        name=project.name,
        creation_time=project.creation_time,
        valid=project.valid,
        active=project.active,
    )


def view(project: ProjectDB, data: AggregatedDataSource) -> Project:
    return Project(
        project_id=project.project_id,
        name=project.name,
        creation_time=project.creation_time,
        token=project.token,
        valid=project.valid,
        active=project.active,
        n_clients=len(set([ds.component_id for ds in project.datasources])),
        n_datasources=len(project.datasources),
        data=data,
    )


class ProjectRepository(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.tr: TokenRepository = TokenRepository(session)
        self.dsr: DataSourceRepository = DataSourceRepository(session)

    async def create(self, name: str = "", token: str | None = None) -> str:

        if token is None:
            token = await self.tr.project_token(name)

        res = await self.session.scalars(select(ProjectDB).where(ProjectDB.token == token))
        p = res.one_or_none()

        if p is not None:
            raise ValueError("A project with the given token already exists")

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
        # TODO: remove this
        try:
            res = await self.session.scalars(select(DataSourceDB).where(DataSourceDB.datasource_id == datasource_id))
            ds: DataSourceDB = res.one()

            res = await self.session.scalars(select(ProjectDB).where(ProjectDB.project_id == project_id))
            p: ProjectDB = res.one()

            p.datasources.append(ds)

            self.session.add(p)
            await self.session.commit()

        except NoReferenceError:
            raise ValueError()

    async def add_datasources_from_metadata(self, metadata: Metadata) -> None:
        for mdds in metadata.datasources:

            res = await self.session.scalars(
                select(DataSourceDB).where(DataSourceDB.datasource_id == mdds.datasource_id)
            )
            ds: DataSourceDB = res.one()

            if not mdds.tokens:
                continue

            res = await self.session.scalars(select(ProjectDB.project_id).filter(ProjectDB.token.in_(mdds.tokens)))
            project_ids: list[str] = list(res.all())

            if not project_ids:
                # TODO: should this be an error?
                continue

            for project_id in project_ids:
                res = await self.session.scalars(
                    select(ProjectDB)
                    .where(ProjectDB.project_id == project_id)
                    .options(selectinload(ProjectDB.datasources))
                )
                p: ProjectDB = res.one()

                p.datasources.append(ds)
                self.session.add(p)

            await self.session.commit()

    async def get_project_list(self) -> list[BaseProject]:
        res = await self.session.execute(select(ProjectDB))
        project_db_list = res.scalars().all()
        return [simpleView(p) for p in project_db_list]

    async def get_by_id(self, project_id: str) -> Project:
        """Can raise NoResultsException."""
        res = await self.session.scalars(
            select(ProjectDB).where(ProjectDB.project_id == project_id).options(selectinload(ProjectDB.datasources))
        )
        p = res.one()

        dss: list[DataSource] = [await self.dsr.load(ds.datasource_id) for ds in p.datasources]

        data = AggregatedDataSource.aggregate(dss)

        return view(p, data)

    async def get_by_token(self, token: str) -> Project:
        """Can raise NoResultsException."""
        res = await self.session.scalars(
            select(ProjectDB).where(ProjectDB.token == token).options(selectinload(ProjectDB.datasources))
        )
        p = res.one()

        dss: list[DataSource] = [await self.dsr.load(ds.datasource_id) for ds in p.datasources]

        data = AggregatedDataSource.aggregate(dss)

        return view(p, data)

    async def client_ids(self, token: str) -> list[str]:
        """Can raise NoResultException"""
        res = await self.session.scalars(
            select(ProjectDB).where(ProjectDB.token == token).options(selectinload(ProjectDB.datasources))
        )

        p = res.one()

        return [ds.component_id for ds in p.datasources]

    async def datasources_ids(self, token: str) -> list[str]:
        """Can raise NoResultException"""
        res = await self.session.scalars(
            select(ProjectDB).where(ProjectDB.token == token).options(selectinload(ProjectDB.datasources))
        )

        p = res.one()

        return [ds.datasource_id for ds in p.datasources]
