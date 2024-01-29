from __future__ import annotations

from ferdelance import __version__
from ferdelance.config import DataSourceStorage, Configuration, config_manager
from ferdelance.const import TYPE_CLIENT, TYPE_USER
from ferdelance.core.artifacts import Artifact, ArtifactStatus
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    DataSourceRepository,
    JobRepository,
    ProjectRepository,
)
from ferdelance.node.services import (
    JobManagementService,
    WorkbenchService,
    TaskManagementService,
    ResourceManagementService,
)
from ferdelance.node.startup import NodeStartup
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import Resource
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.project import Project
from ferdelance.schemas.resources import ResourceIdentifier
from ferdelance.schemas.updates import UpdateData
from ferdelance.tasks.services.execution import load_environment
from ferdelance.tasks.tasks import Task, TaskError

from tests.utils import create_project

from sqlalchemy.ext.asyncio import AsyncSession


class ServerlessWorker:
    def __init__(
        self,
        index: int,
        node: ServerlessExecution,
        data: DataSourceStorage | Metadata | None = None,
    ) -> None:
        self.index: int = index

        self.node: ServerlessExecution = node
        self.config: Configuration = config_manager.get()

        self.md: Metadata | None = None
        self.data: DataSourceStorage | None = None

        if isinstance(data, DataSourceStorage):
            self.data = data
            self.md = data.metadata()

        if isinstance(data, Metadata):
            self.md = data

        self.component: Component

    async def setup(self, cr: ComponentRepository, component: Component | None = None):
        if component is None:
            self.component = await cr.create_component(
                f"client-{self.index}",
                TYPE_CLIENT,
                f"key-{self.index}",
                __version__,
                f"client-{self.index}",
                f"ip-{self.index}",
                "",
            )
        else:
            self.component = component

    def has_metadata(self) -> bool:
        return self.md is not None

    def metadata(self) -> Metadata:
        if self.md is None:
            raise ValueError("This client ha been created without metadata")
        return self.md

    async def next_action(self) -> UpdateData:
        return await self.node.get_update(self.component)

    async def get_task(self, next_action: UpdateData) -> Task:
        return await self.node.get_task(next_action.job_id)

    async def complete_task(self, task: Task) -> None:
        await self.node.task_completed(task)

    async def execute(self, task: Task) -> Resource:
        if self.data is None:
            raise ValueError("No data to working on available")

        env = load_environment(self.data, task, self.config.get_workdir())

        for resource in task.required_resources:
            env.add_resource(
                resource.resource_id,
                self.config.storage_job(
                    resource.artifact_id,
                    resource.job_id,
                    resource.iteration,
                )
                / f"{resource.resource_id}.pkl",
            )

        env = task.run(env)

        assert env.products is not None

        return Resource(
            id=env.product_id,
            component_id=self.component.id,
            creation_time=None,
            path=env.product_path(),
            is_external=False,
            is_error=False,
            is_ready=True,
        )

    async def next_get_execute_post(self) -> Resource:
        next_action = await self.next_action()

        if next_action is None:
            raise ValueError("next_action is not an execution action!")

        task = await self.get_task(next_action)
        res = await self.execute(task)
        await self.complete_task(task)

        return res


class ServerlessExecution:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.cr: ComponentRepository = ComponentRepository(session)
        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.jr: JobRepository = JobRepository(session)

        self.dsr: DataSourceRepository = DataSourceRepository(self.session)
        self.pr: ProjectRepository = ProjectRepository(self.session)

        self.workers: dict[str, ServerlessWorker] = dict()
        self.self_component: Component
        self.user_component: Component

        self.jobs_service: JobManagementService
        self.task_service: TaskManagementService
        self.workbench_service: WorkbenchService
        self.resource_service: ResourceManagementService

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

        self.self_worker = await self.add_worker(component=self.self_component)

        self.jobs_service = JobManagementService(self.session, self.self_component)
        self.task_service = TaskManagementService(self.session, self.self_component, "", "")
        self.workbench_service = WorkbenchService(self.session, self.user_component, self.self_component)
        self.resource_service = ResourceManagementService(self.session)

    async def add_worker(
        self,
        data: DataSourceStorage | Metadata | None = None,
        component: Component | None = None,
    ) -> ServerlessWorker:
        sw = ServerlessWorker(len(self.workers), self, data)
        await sw.setup(self.cr, component)

        if sw.has_metadata():
            metadata = sw.metadata()

            await self.cr.create_event(sw.component.id, "update metadata")

            # this will also update existing metadata
            await self.dsr.create_or_update_from_metadata(sw.component.id, metadata)
            await self.pr.add_datasources_from_metadata(metadata)

        self.workers[sw.component.id] = sw

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

    async def task_completed(self, task: Task) -> None:
        await self.jobs_service.task_completed(task.job_id)
        await self.jobs_service.check(task.artifact_id)

    async def task_failed(self, job_id: str) -> None:
        await self.jobs_service.task_failed(TaskError(job_id=job_id))

    async def get_resource(self, resource_id: str) -> Resource:
        return await self.resource_service.load_resource(ResourceIdentifier(resource_id=resource_id))
