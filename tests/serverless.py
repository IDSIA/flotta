from __future__ import annotations

from ferdelance import __version__
from ferdelance.config import DataSourceStorage
from ferdelance.const import TYPE_CLIENT, TYPE_USER
from ferdelance.core.artifacts import Artifact, ArtifactStatus
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    DataSourceRepository,
    JobRepository,
    ProjectRepository,
)
from ferdelance.node.services import JobManagementService, WorkbenchService
from ferdelance.node.services.tasks import TaskManagementService
from ferdelance.node.startup import NodeStartup
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import Resource
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.project import Project
from ferdelance.schemas.updates import UpdateData
from ferdelance.tasks.tasks import Task, TaskError

from tests.utils import create_project

from sqlalchemy.ext.asyncio import AsyncSession


class ServerlessWorker:
    def __init__(
        self,
        session: AsyncSession,
        index: int,
        node: ServerlessExecution,
        data: DataSourceStorage | Metadata | None = None,
    ) -> None:
        self.session: AsyncSession = session
        self.index: int = index

        self.node: ServerlessExecution = node

        self.md: Metadata | None = None
        self.data: DataSourceStorage | None = None

        if isinstance(data, DataSourceStorage):
            self.data = data
            self.md = data.metadata()

        if isinstance(data, Metadata):
            self.md = data

        self.cr: ComponentRepository = ComponentRepository(session)

        self.worker: Component

    async def setup(self):
        self.worker = await self.cr.create_component(
            f"client-{self.index}",
            TYPE_CLIENT,
            f"key-{self.index}",
            __version__,
            f"client-{self.index}",
            f"ip-{self.index}",
            "",
        )

    def metadata(self) -> Metadata:
        if self.md is None:
            raise ValueError("This client ha been created without metadata")
        return self.md

    async def next_action(self) -> UpdateData:
        return await self.node.get_update(self.worker)

    async def get_task(self, next_action: UpdateData) -> Task:
        return await self.node.get_task(next_action.job_id)

    async def post_resource(self, task: Task) -> None:
        await self.node.task_completed(task)

    async def execute(self, task: Task) -> Resource:
        # TODO
        raise NotImplementedError()

    async def next_get_execute_post(self) -> Resource:
        next_action = await self.next_action()

        if not isinstance(next_action, UpdateData):
            raise ValueError("next_action is not an execution action!")

        task = await self.get_task(next_action)
        res = await self.execute(task)
        await self.post_resource(task)

        return res


class ServerlessExecution:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.cr: ComponentRepository = ComponentRepository(session)
        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.jr: JobRepository = JobRepository(session)

        self.workers: dict[str, ServerlessWorker] = dict()
        self.self_component: Component
        self.user_component: Component

        self.jobs_service: JobManagementService
        self.task_service: TaskManagementService
        self.workbench_service: WorkbenchService

    async def setup(self):
        await NodeStartup(self.session).startup()

        self.self_component = await self.cr.get_self_component()
        self.user_component = await self.cr.create_component(
            "user-1",
            TYPE_USER,
            "user-1-public_key",
            __version__,
            "user-1",
            "ip-user-1",
            "",
        )

        self.jobs_service = JobManagementService(self.session, self.self_component)
        self.task_service = TaskManagementService(self.session, self.self_component)
        self.workbench_service = WorkbenchService(self.session, self.user_component, self.self_component)

    async def add_worker(self, index: int, data: DataSourceStorage | Metadata) -> ServerlessWorker:
        sw = ServerlessWorker(self.session, index, self, data)
        await sw.setup()

        metadata = sw.metadata()

        dsr: DataSourceRepository = DataSourceRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        await self.cr.create_event(sw.worker.id, "update metadata")

        # this will also update existing metadata
        await dsr.create_or_update_from_metadata(sw.worker.id, metadata)
        await pr.add_datasources_from_metadata(metadata)

        self.workers[sw.worker.id] = sw

        return sw

    async def create_project(self, project_token: str) -> None:
        await create_project(self.session, project_token)

    async def get_project(self, project_token: str) -> Project:
        return await self.workbench_service.project(project_token)

    async def submit(self, artifact: Artifact) -> str:
        status = await self.workbench_service.submit_artifact(artifact)

        assert status.id is not None

        return status.id

    async def get_status(self, artifact_id: str) -> ArtifactStatus:
        return await self.workbench_service.get_status_artifact(artifact_id)

    async def get_update(self, component: Component):
        return await self.jobs_service.update(component)

    async def next(self, component: Component) -> str | None:
        return await self.jobs_service.next_task_for_component(component.id)

    async def get_task(self, job_id: str) -> Task:
        return await self.jobs_service.get_task_by_job_id(job_id)

    async def start_task(self, task: Task) -> None:
        await self.task_service.task_start(task)

    async def task_completed(self, task: Task) -> None:
        await self.jobs_service.task_completed(task.job_id)

    async def task_failed(self, job_id: str) -> None:
        await self.jobs_service.task_failed(TaskError(job_id=job_id))

    async def get_resource(self, resource_id: str) -> Resource:
        return await self.jobs_service.load_resource(resource_id)
