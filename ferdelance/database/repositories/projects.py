from ferdelance.database.repositories.core import AsyncSession, Repository
from ferdelance.database.repositories.datasource import DataSourceRepository
from ferdelance.database.tables import (
    DataSource as DataSourceDB,
    Project as ProjectDB,
)
from ferdelance.logging import get_logger
from ferdelance.schemas.datasources import DataSource
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.project import (
    Project,
    BaseProject,
    AggregatedDataSource,
)

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from hashlib import sha256
from time import time
from uuid import uuid4

LOGGER = get_logger(__name__)


def simpleView(project: ProjectDB) -> BaseProject:
    return BaseProject(
        id=project.id,
        token=project.token,
        name=project.name,
        creation_time=project.creation_time,
        valid=project.valid,
        active=project.active,
    )


def view(project: ProjectDB, data: AggregatedDataSource) -> Project:
    return Project(
        id=project.id,
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
    """A repository for all the projects stored in the database.

    A project is a collection both of datasources and clients that share the same
    goal.
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.dsr: DataSourceRepository = DataSourceRepository(session)

    async def generate_project_token(self, name: str, encoding: str = "utf8") -> str:
        """Project have tokens assigned to them. The token is based on the name
        of the project. Project's tokens are intended to not be replaceable.

        Args:
            name (str):
                The name of the new project.

        Returns:
            str:
                A string to use as a token.
        """

        LOGGER.info("generating token for new project")

        ms = round(time() * 1000 + 7)
        salt = str(uuid4())[:17]

        token_b: bytes = f"{ms}¨{name}${salt};".encode(encoding)
        token_b: bytes = sha256(token_b).hexdigest().encode(encoding)
        token: str = sha256(token_b).hexdigest()

        return token

    async def create_project(self, name: str, token: str | None = None) -> str:
        """Create a new project. It is possible to assign a name and a token.
        If none of them is provided, they will be generated. If the name or
        the token already exists, an exception is raised.

        Args:
            name (str):
                Name of the project.
            token (str | None, optional):
                Token to use. If None, a new one will be generated.
                Defaults to None.

        Raises:
            ValueError: If exists a project with the same name or token .

        Returns:
            str:
                The token to insert to use this project.
        """

        if token is None:
            token = await self.generate_project_token(name)

        res = await self.session.scalars(
            select(ProjectDB).where(
                (ProjectDB.token == token) | (ProjectDB.name == name),
            )
        )
        p = res.one_or_none()

        if p is not None:
            raise ValueError("A project with the given token already exists")

        project = ProjectDB(
            id=str(uuid4()),
            name=name,
            token=token,
        )

        self.session.add(project)
        await self.session.commit()

        return token

    async def add_datasources_from_metadata(self, metadata: Metadata) -> None:
        """Read the metadata received from a client and add the datasources to
        projects as described in the metadata itself.

        If a datasource does not have an associated project, the datasource will
        be ignored and not used by the workbenches.

        Args:
            metadata (Metadata):
                Metadata object received from a client.
        """
        for mdds in metadata.datasources:
            res = await self.session.scalars(select(DataSourceDB).where(DataSourceDB.id == mdds.id))
            ds: DataSourceDB = res.one()

            if not mdds.tokens:
                LOGGER.warn(f"no tokens assigned to datasource={mdds.id}")
                continue

            res = await self.session.scalars(select(ProjectDB).filter(ProjectDB.token.in_(mdds.tokens)))
            projects: list[ProjectDB] = list(res.all())
            project_tokens: list[str] = [p.token for p in projects]
            project_ids: list[str] = [p.id for p in projects]

            missing_tokens = []
            for token in mdds.tokens:
                if token not in project_tokens:
                    missing_tokens.append(token)

            if missing_tokens:
                LOGGER.warn(
                    f"{len(missing_tokens)} project token(s) not found for "
                    f"datasource={mdds.id} datasource_hash={mdds.hash}"
                )

                for new_token in missing_tokens:
                    project_id = await self.create_project("", new_token)

                    LOGGER.info(f"created new project={project_id} for unknown token={new_token}")

                    project_ids.append(project_id)

            for project_id in project_ids:
                res = await self.session.scalars(
                    select(ProjectDB).where(ProjectDB.id == project_id).options(selectinload(ProjectDB.datasources))
                )
                p: ProjectDB = res.one()

                p.datasources.append(ds)
                self.session.add(p)

            await self.session.commit()

    async def list_projects(self) -> list[BaseProject]:
        """Return the list of all projects. The returned list is of BaseProject,
        these objects have no information regarding the assigned data sources or
        the clients.

        Returns:
            list[BaseProject]:
                A list of all the projects in handler format. NOte that the list
                can be empty.
        """
        res = await self.session.execute(select(ProjectDB))
        project_db_list = res.scalars().all()
        return [simpleView(p) for p in project_db_list]

    async def get_by_id(self, project_id: str) -> Project:
        """Return the project associated with the given id.

        Args:
            project_id (str):
                Id of the project to return.

        Raises:
            NoResultFound:
                If there is no project associated with the given id.

        Returns:
            Project:
                The full handler of the requested project id.
        """

        res = await self.session.scalars(
            select(ProjectDB).where(ProjectDB.id == project_id).options(selectinload(ProjectDB.datasources))
        )
        p = res.one()

        dss: list[DataSource] = [await self.dsr.load(ds.id) for ds in p.datasources]

        data = AggregatedDataSource.aggregate(dss)

        return view(p, data)

    async def get_by_token(self, token: str) -> Project:
        """Return the project associated with the given token.

        Args:
            project_id (str):
                Token of the project to return.

        Raises:
            NoResultFound:
                If there is no project associated with the given token.

        Returns:
            Project:
                The full handler of the requested project token.
        """
        res = await self.session.scalars(
            select(ProjectDB).where(ProjectDB.token == token).options(selectinload(ProjectDB.datasources))
        )
        p = res.one()

        dss: list[DataSource] = [await self.dsr.load(ds.id) for ds in p.datasources]

        data = AggregatedDataSource.aggregate(dss)

        return view(p, data)

    async def list_client_ids(self, token: str) -> list[str]:
        """Returns a list of all the client ids that can contribute to the project,
        identified by the give project token.

        Args:
            token (str):
                Project token to search for.

        Raises:
            NoResultFound:
                If there is no project associated with the given token.

        Returns:
            list[str]:
                A list of all the client ids that contribute to the project.
                Note that this list could be empty.
        """
        res = await self.session.scalars(
            select(ProjectDB).where(ProjectDB.token == token).options(selectinload(ProjectDB.datasources))
        )

        p = res.one()

        return [ds.component_id for ds in p.datasources]

    async def list_datasources_ids(self, token: str) -> list[str]:
        """Returns a list of all the datasources ids that can contribute to the
        project, identified by the give project token.

        Args:
            token (str):
                Project token to search for.

        Raises:
            NoResultFound:
                If there is no project associated with the given token.

        Returns:
            list[str]:
                A list of all the datasource ids that contribute to the project.
                Note that this list could be an empty list.
        """
        res = await self.session.scalars(
            select(ProjectDB).where(ProjectDB.token == token).options(selectinload(ProjectDB.datasources))
        )

        p = res.one()

        return [ds.id for ds in p.datasources]
