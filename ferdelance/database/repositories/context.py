from ferdelance.database.repositories import (
    AsyncSession,
    ArtifactRepository,
    DataSourceRepository,
    JobRepository,
    ResultRepository,
    ComponentRepository,
    ProjectRepository,
)


class AggregationContext:
    def __init__(self, session: AsyncSession) -> None:
        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.cr: ComponentRepository = ComponentRepository(session)
        self.dsr: DataSourceRepository = DataSourceRepository(session)
        self.jr: JobRepository = JobRepository(session)
        self.pr: ProjectRepository = ProjectRepository(session)
        self.rr: ResultRepository = ResultRepository(session)
