from typing import Callable

from ferdelance.database.data import TYPE_WORKER, TYPE_USER
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    JobRepository,
)
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.components import Client, Token
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

from tests.utils import create_project

from sqlalchemy.ext.asyncio import AsyncSession


class ServerlessClient:
    def __init__(self, session: AsyncSession, i: int) -> None:
        self.session: AsyncSession = session
        self.i: int = i

        self.cr: ComponentRepository = ComponentRepository(session)
        self.client: Client = None  # type: ignore
        self.node_service: NodeService = None  # type: ignore
        self.token: Token = None  # type: ignore

    async def setup(self):
        self.client, self.token = await self.cr.create_client(
            f"client-{self.i}",
            "1",
            f"key-{self.i}",
            f"sys-{self.i}",
            f"mac-{self.i}",
            f"node-{self.i}",
            f"ip-{self.i}",
        )
        self.node_service = NodeService(self.session, self.client)
        self.client_service = ClientService(self.session, self.client)

    async def next_action(self) -> UpdateClientApp | UpdateExecute | UpdateNothing | UpdateToken:
        return await self.client_service.update({})

    async def get_client_task(self, next_action: UpdateExecute) -> ClientTask:
        return await self.client_service.get_task(next_action)

    async def post_client_results(self, task: ClientTask) -> Result:
        return await self.client_service.result(task.job_id)

    # TODO: these two methods should go under a "ServerService"
    async def check_aggregation(self, result: Result) -> bool:
        return await self.client_service.check(result)

    async def aggregate(self, result: Result, start_function: Callable[[str, str, list[str], str], str]) -> Job:
        return await self.client_service.start_aggregation(result, start_function)


class ServerlessExecution:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.cr: ComponentRepository = ComponentRepository(session)
        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.jr: JobRepository = JobRepository(session)

        self.clients: dict[str, ServerlessClient] = dict()

        self.worker_service: WorkerService = None  # type: ignore
        self.workbench_service: WorkbenchService = None  # type: ignore

    async def setup(self):
        await ServerStartup(self.session).startup()

        worker_component, _ = await self.cr.create_component(TYPE_WORKER, "worker-1")
        user_component, _ = await self.cr.create_component(TYPE_USER, "user-1")

        self.worker_service = WorkerService(self.session, worker_component)
        self.workbench_service = WorkbenchService(self.session, user_component)

    async def add_client(self, index: int, metadata: Metadata) -> ServerlessClient:
        sc = ServerlessClient(self.session, index)
        await sc.setup()
        await sc.node_service.metadata(metadata)

        self.clients[sc.client.id] = sc

        return sc

    async def create_project(self, project_token: str):
        await create_project(self.session, project_token)

    async def get_project(self, project_token: str) -> Project:
        return await self.workbench_service.project(project_token)

    async def submit(self, artifact: Artifact) -> str:
        status = await self.workbench_service.submit_artifact(artifact)

        assert status.id is not None

        return status.id

    async def get_worker_task(self, job: Job) -> WorkerTask:
        return await self.worker_service.get_task(job.id)

    async def post_worker_result(self, job: Job):
        return await self.worker_service.completed(job.id)
