from typing import Callable

from ferdelance.client.config import DataConfig
from ferdelance.database.data import TYPE_WORKER, TYPE_USER
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    JobRepository,
)
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.components import Client, Token
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
from ferdelance.schemas.worker import TaskArguments, TaskExecutionParameters, TaskAggregationParameters
from ferdelance.server.services import ClientService, NodeService, WorkerService, WorkbenchService
from ferdelance.server.startup import ServerStartup
from ferdelance.worker.jobs.actors import ExecutionResult, run_estimate, run_training

from tests.utils import create_project

from sqlalchemy.ext.asyncio import AsyncSession

import aiofiles.os


class ServerlessClient:
    def __init__(self, session: AsyncSession, index: int, data: DataConfig | Metadata | None = None) -> None:
        self.session: AsyncSession = session
        self.index: int = index

        self.md: Metadata | None = None
        self.data: DataConfig | None = None

        if isinstance(data, DataConfig):
            self.data = data
            self.md = data.metadata()

        if isinstance(data, Metadata):
            self.md = data

        self.cr: ComponentRepository = ComponentRepository(session)

        self.client: Client = None  # type: ignore
        self.node_service: NodeService = None  # type: ignore
        self.token: Token = None  # type: ignore

    async def setup(self):
        self.client, self.token = await self.cr.create_client(
            f"client-{self.index}",
            "1",
            f"key-{self.index}",
            f"sys-{self.index}",
            f"mac-{self.index}",
            f"node-{self.index}",
            f"ip-{self.index}",
        )
        self.node_service = NodeService(self.session, self.client)
        self.client_service = ClientService(self.session, self.client)

    def metadata(self) -> Metadata:
        if self.md is None:
            raise ValueError("This client ha been created without metadata")
        return self.md

    async def next_action(self) -> UpdateClientApp | UpdateExecute | UpdateNothing | UpdateToken:
        return await self.client_service.update({})

    async def get_client_task(self, next_action: UpdateExecute) -> TaskExecutionParameters:
        return await self.client_service.get_task(next_action)

    async def post_client_results(
        self, task: TaskExecutionParameters, in_result: ExecutionResult | None = None
    ) -> Result:
        result = await self.client_service.task_completed(task.job_id)

        if in_result is not None:
            async with aiofiles.open(in_result.path, "rb") as src:
                async with aiofiles.open(result.path, "wb") as dst:
                    while (chunk := await src.read()) != b"":
                        await dst.write(chunk)

        return result

    async def execute(self, task: TaskExecutionParameters) -> ExecutionResult:
        if self.data is None:
            raise ValueError("Cannot execute job without local data configuration.")

        if task.artifact.is_estimation():
            return run_estimate(self.data, task)

        if task.artifact.is_model():
            return run_training(self.data, task)

        raise ValueError("Invalid artifact.")

    async def next_get_execute_post(self) -> Result:
        next_action = await self.next_action()

        if not isinstance(next_action, UpdateExecute):
            raise ValueError("next_action is not an execution action!")

        task = await self.get_client_task(next_action)
        res = await self.execute(task)
        result = await self.post_client_results(task, res)

        return result


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

    async def add_client(self, index: int, data: DataConfig | Metadata) -> ServerlessClient:
        sc = ServerlessClient(self.session, index, data)
        await sc.setup()
        await sc.node_service.metadata(sc.metadata())

        self.clients[sc.client.id] = sc

        return sc

    async def create_project(self, project_token: str) -> None:
        await create_project(self.session, project_token)

    async def get_project(self, project_token: str) -> Project:
        return await self.workbench_service.project(project_token)

    async def submit(self, artifact: Artifact) -> str:
        status = await self.workbench_service.submit_artifact(artifact)

        assert status.id is not None

        return status.id

    async def check_aggregation(self, result: Result) -> bool:
        return await self.clients[result.client_id].client_service.check(result)

    async def aggregate(self, result: Result, start_function: Callable[[TaskArguments], str]) -> Job:
        return await self.clients[result.client_id].client_service.start_aggregation(result, start_function)

    async def get_worker_task(self, job: Job) -> TaskAggregationParameters:
        return await self.worker_service.get_task(job.id)

    async def post_worker_result(self, job: Job):
        result = await self.worker_service.aggregation_completed(job.id)
        await self.worker_service.check_next_iteration(job.id)
        return result


class ServerlessWorker:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # TODO:
