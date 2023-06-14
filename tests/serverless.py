from typing import Callable

from ferdelance.database.data import TYPE_WORKER, TYPE_SERVER
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    JobRepository,
)
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.database import Result
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.project import Project
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.updates import (
    UpdateClientApp,
    UpdateExecute,
    UpdateNothing,
    UpdateToken,
)
from ferdelance.schemas.worker import WorkerTask
from ferdelance.server.services import ClientService, NodeService, WorkerService, WorkbenchService
from ferdelance.server.startup import ServerStartup

from tests.utils import (
    create_project,
    get_metadata,
)

from sqlalchemy.ext.asyncio import AsyncSession


class ServerlessExecution:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.cr: ComponentRepository = ComponentRepository(session)
        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.jr: JobRepository = JobRepository(session)

        self.node_service: NodeService = None  # type:ignore
        self.client_service: ClientService = None  # type: ignore
        self.worker_service: WorkerService = None  # type: ignore
        self.workbench_service: WorkbenchService = None  # type: ignore

    async def setup(self, project_token):
        await ServerStartup(self.session).startup()
        await create_project(self.session, project_token)

        client_component, _ = await self.cr.create_client("client-1", "1", "2", "3", "4", "5", "6")
        worker_component, _ = await self.cr.create_component(TYPE_WORKER, "worker-1")

        self.node_service = NodeService(self.session, client_component)
        self.client_service = ClientService(self.session, client_component.client_id)
        self.worker_service = WorkerService(self.session, worker_component.component_id)
        self.workbench_service = WorkbenchService(self.session, "workbench-1")

        metadata = get_metadata()
        await self.add_project(metadata)

    async def add_project(self, metadata: Metadata) -> Metadata:
        return await self.node_service.metadata(metadata)

    async def get_project(self, project_token: str) -> Project:
        return await self.workbench_service.project(project_token)

    async def submit(self, artifact: Artifact) -> str:
        status = await self.workbench_service.submit_artifact(artifact)

        assert status.artifact_id is not None

        return status.artifact_id

    async def next_action(self) -> UpdateClientApp | UpdateExecute | UpdateNothing | UpdateToken:
        return await self.client_service.update({})

    async def get_client_task(self, next_action: UpdateExecute) -> ClientTask:
        return await self.client_service.get_task(next_action)

    async def post_client_results(self, task: ClientTask) -> Result:
        return await self.client_service.result(task.job_id)

    async def check_aggregation(self, result: Result) -> bool:
        return await self.client_service.check(result)

    async def aggregate(self, result: Result, start_function: Callable[[str, str, list[str], str], str]) -> Job:
        return await self.client_service.start_aggregation(result, start_function)

    async def get_worker_task(self, job: Job) -> WorkerTask:
        return await self.worker_service.get_task(job.job_id)

    async def post_worker_result(self, job: Job):
        return await self.worker_service.completed(job.job_id)
