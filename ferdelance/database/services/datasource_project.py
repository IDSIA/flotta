from sqlalchemy import select

from ferdelance.database.services.core import AsyncSession, DBSessionService
from ferdelance.database.services.datasource import DataSourceService
from ferdelance.database.services.projects import ProjectService
from ferdelance.database.tables import DataSource as DataSourceDB
from ferdelance.database.tables import Project as ProjectDB
from ferdelance.database.tables import ProjectDataSource as PDSDB
from ferdelance.database.schemas import DataSource as DataSourceView
from ferdelance.database.schemas import Project as ProjectView


class DataSourceProjectService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.dss: DataSourceService = DataSourceService(session)
        self.ps: ProjectService = ProjectService(session)

    async def get_datasources_by_project(self, project_token: str) -> list[DataSourceView]:
        """Can raise NoResultsException."""
        res = await self.session.scalars(
            select(DataSourceDB)
            .join(ProjectDB, ProjectDB.project_id == PDSDB.project_id)
            .join(PDSDB, PDSDB.datasource_id == DataSourceDB.datasource_id)
            .where(ProjectDB.token == project_token)
        )
        return list(res.all())
