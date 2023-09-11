from typing import Callable

from ferdelance import __version__
from ferdelance.client.state import DataSourceStorage
from ferdelance.const import TYPE_CLIENT, TYPE_USER
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    JobRepository,
)
from ferdelance.node.services import ComponentService, NodeService, WorkerService, WorkbenchService
from ferdelance.node.startup import NodeStartup
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import Result
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.project import Project
from ferdelance.schemas.tasks import TaskArguments, TaskParameters
from ferdelance.schemas.updates import UpdateData
from ferdelance.tasks.jobs.actors import ExecutionResult, run_estimate, run_training

from tests.utils import create_project

from sqlalchemy.ext.asyncio import AsyncSession

import aiofiles.os


class ServerlessClient:
    def __init__(self, session: AsyncSession, index: int, data: DataSourceStorage | Metadata | None = None) -> None:
        self.session: AsyncSession = session
        self.index: int = index

        self.md: Metadata | None = None
        self.data: DataSourceStorage | None = None

        if isinstance(data, DataSourceStorage):
            self.data = data
            self.md = data.metadata()

        if isinstance(data, Metadata):
            self.md = data

        self.cr: ComponentRepository = ComponentRepository(session)

        self.client: Component
        self.node_service: NodeService

    async def setup(self):
        self.client = await self.cr.create_component(
            f"client-{self.index}",
            TYPE_CLIENT,
            f"key-{self.index}",
            __version__,
            f"client-{self.index}",
            f"ip-{self.index}",
            "",
        )
        self.node_service = NodeService(self.session, self.client)
        self.client_service = ComponentService(self.session, self.client)

    def metadata(self) -> Metadata:
        if self.md is None:
            raise ValueError("This client ha been created without metadata")
        return self.md

    async def next_action(self) -> UpdateData:
        return await self.client_service.update()

    async def get_client_task(self, job_id: str) -> TaskParameters:
        return await self.client_service.get_task(job_id)

    async def post_client_results(self, task: TaskParameters, in_result: ExecutionResult | None = None) -> Result:
        result = await self.client_service.task_completed(task.job_id)

        if in_result is not None:
            async with aiofiles.open(in_result.path, "rb") as src:
                async with aiofiles.open(result.path, "wb") as dst:
                    while (chunk := await src.read()) != b"":
                        await dst.write(chunk)

        return result

    async def execute(self, task: TaskParameters) -> ExecutionResult:
        if self.data is None:
            raise ValueError("Cannot execute job without local data configuration.")

        if task.artifact.is_estimation():
            return run_estimate(self.data, task)

        if task.artifact.is_model():
            return run_training(self.data, task)

        raise ValueError("Invalid artifact.")

    async def next_get_execute_post(self) -> Result:
        next_action = await self.next_action()

        if not isinstance(next_action, UpdateData):
            raise ValueError("next_action is not an execution action!")

        task = await self.get_client_task(next_action.job_id)
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

        self.worker_service: WorkerService
        self.workbench_service: WorkbenchService

    async def setup(self):
        await NodeStartup(self.session).startup()

        self_component = await self.cr.get_self_component()
        user_component = await self.cr.create_component(
            "user-1",
            TYPE_USER,
            "user-1-public_key",
            __version__,
            "user-1",
            "ip-user-1",
            "",
        )

        self.worker_service = WorkerService(self.session, self_component)
        self.workbench_service = WorkbenchService(self.session, user_component)

    async def add_client(self, index: int, data: DataSourceStorage | Metadata) -> ServerlessClient:
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

    async def aggregate(self, result: Result, start_function: Callable[[TaskArguments], None]) -> Job:
        return await self.clients[result.client_id].client_service.start_aggregation(result, start_function)

    async def get_worker_task(self, job: Job) -> TaskParameters:
        return await self.worker_service.get_task(job.id)

    async def post_worker_result(self, job: Job):
        result = await self.worker_service.aggregation_completed(job.id)
        await self.worker_service.check_next_iteration(job.id)
        return result


class ServerlessWorker:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # TODO:
