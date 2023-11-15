from typing import Sequence
from ferdelance.core.artifacts import Artifact, ArtifactStatus
from ferdelance.core.interfaces import SchedulerContext, SchedulerJob
from ferdelance.core.metrics import Metrics
from ferdelance.database.repositories import (
    AsyncSession,
    ArtifactRepository,
    ComponentRepository,
    DataSourceRepository,
    JobRepository,
    ProjectRepository,
    ResourceRepository,
    Repository,
)
from ferdelance.logging import get_logger
from ferdelance.node.services import ActionService, TaskManagementService
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import ServerArtifact, Resource
from ferdelance.schemas.jobs import Job
from ferdelance.tasks.tasks import Task, TaskError, TaskNode, TaskResource
from ferdelance.schemas.updates import UpdateData
from ferdelance.shared.status import JobStatus, ArtifactJobStatus

from sqlalchemy.exc import NoResultFound

import aiofiles
import aiofiles.ospath
import json
import os

LOGGER = get_logger(__name__)


class JobManagementService(Repository):
    def __init__(
        self,
        session: AsyncSession,
        component: Component,
        private_key: str = "",
        node_public_key: str = "",
    ) -> None:
        super().__init__(session)

        self.component: Component = component
        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.ax: ActionService = ActionService(self.session)
        self.cr: ComponentRepository = ComponentRepository(session)
        self.dsr: DataSourceRepository = DataSourceRepository(session)
        self.jr: JobRepository = JobRepository(session)
        self.pr: ProjectRepository = ProjectRepository(session)
        self.rr: ResourceRepository = ResourceRepository(session)

        self.tm: TaskManagementService = TaskManagementService(session, component)

        self.private_key = private_key
        self.node_public_key = node_public_key

    async def update(self) -> UpdateData:
        next_action = await self.ax.next(self.component)

        LOGGER.debug(f"component={self.component.id}: update action={next_action.action}")

        return next_action

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        """The submitted artifact will be stored on disk after an handler has been created in the database.

        If everything has been created successfully, then multiple jobs will be created for the clients. Otherwise, a
        ValueError exception will be raised.

        The amount of jobs created depends on the submitted artifact, and the number of data sources available in the
        relative project.

        Args:
            artifact (Artifact):
                New artifact to save on the database, on disk, and then schedule.

        Raises:
            ValueError:
                If there was an error in creating the handlers on the database or on disk.

        Returns:
            ArtifactStatus:
                Handler to manage artifact.
        """
        try:
            artifact_db: ServerArtifact = await self.ar.create_artifact(artifact)

            artifact.id = artifact_db.id

            await self.schedule_tasks_for_iteration(artifact, 0)

            return artifact_db.get_status()
        except ValueError as e:
            raise e

    async def schedule_tasks_for_iteration(self, artifact: Artifact, iteration: int) -> None:
        """Schedules all the jobs for the given artifact in the given iteration.

        Args:
            artifact (Artifact):
                Artifact to complete.
            iteration (int):
                Current iteration.
        """
        project = await self.pr.get_by_id(artifact.project_id)
        datasources_ids = await self.pr.list_datasources_ids(project.token)

        workers: list[Component] = []
        for datasource_id in datasources_ids:
            worker: Component = await self.dsr.get_client_by_datasource_id(datasource_id)
            workers.append(worker)

        LOGGER.info(f"artifact={artifact.id}: creating jobs with {len(workers)} worker(s) for it={iteration}")

        context = SchedulerContext(
            artifact_id=artifact.id,
            initiator=self.component,
            workers=workers,
        )

        jobs: Sequence[SchedulerJob] = artifact.jobs(context)

        LOGGER.info(f"artifact={artifact.id}: planned to schedule {len(jobs)} job(s) for it={iteration}")

        job_map: dict[int, Job] = dict()

        # insert jobs in database
        for job in jobs:
            job_db = await self.jr.create_job(
                artifact.id,
                job,
            )
            job_map[job.id] = job_db

            await self.rr.create_resource(job_db.id, artifact.id, job.worker.id, iteration)

        # insert unlocks in database
        for job in jobs:
            job_db = job_map[job.id]
            unlocks = [job_map[i] for i in job.locks]

            await self.jr.add_locks(job_db, unlocks)

        # set first job to scheduled
        if iteration == 0:
            jobs_ready = await self.jr.list_unlocked_jobs_by_artifact_id(artifact.id)

            for job in jobs_ready:
                LOGGER.info(f"artifact={artifact.id}: scheduling initial job={job.id}")
                await self.jr.schedule_job(job)

        # TODO: how to start this?
        #     get_jobs_backend().start_init(
        #         artifact_id=artifact.id,
        #         job_id=job.id,
        #         component_id=self.component.id,
        #         private_key=self.private_key,
        #         node_url=config_manager.get().url_extern(),
        #         node_public_key=self.node_public_key,
        #     )

    async def next_task_for_component(self, component_id: str) -> str | None:
        jobs = await self.jr.list_scheduled_jobs_for_component(component_id)

        if len(jobs) < 1:
            return None

        return jobs[0].id

    async def store_resource(self, job_id: str, is_error: bool = False) -> Resource:
        job = await self.jr.get_by_id(job_id)

        # simple check that the artifact exists
        await self.ar.get_artifact(job.artifact_id)

        await self.rr.mark_as_ready_by_job_id(job_id, is_error)

        return await self.rr.get_by_job_id(job_id)

    async def load_resource(self, resource_id: str) -> Resource:
        """
        :raise:
            NoResultFound if there is no resource on the disk.
        """
        resource: Resource = await self.rr.get_by_id(resource_id)

        if not os.path.exists(resource.path):
            raise NoResultFound()

        return resource

    async def check(self, artifact_id: str) -> None:
        jobs = await self.jr.list_jobs_by_artifact_id(artifact_id)

        job_to_start = False

        for job in jobs:
            if job.status == JobStatus.WAITING.name:
                job = await self.jr.schedule_job(job)
                job_to_start = True

        if not job_to_start:
            await self.ar.update_status(artifact_id, ArtifactJobStatus.COMPLETED)
            # TODO: check for iteration

    async def get_task_by_job_id(self, job_id: str) -> Task:
        # TODO: add checks if who is downloading the job is allowed to do so
        # FIXME: this is bad written: multiple queries can be aggregate together

        job = await self.jr.get_by_id(job_id)
        scheduler_job = await self.jr.load(job)
        artifact = await self.ar.load(job.artifact_id)

        # collect required resources
        prev_jobs = await self.jr.list_previous_jobs(job.id)

        task_resources = []

        for p_job in prev_jobs:
            c = await self.cr.get_by_id(p_job.component_id)
            r = await self.rr.get_by_job_id(p_job.id)

            task_resources.append(
                TaskResource(
                    component_id=c.id,
                    artifact_id=job.artifact_id,
                    job_id=job.id,
                    public_key=c.public_key,
                    url=c.url,
                    is_local=c.id == self.component.id,  # TODO: should this be a self_component instead?
                    resource_id=r.id,
                )
            )

        # collect next resources
        next_jobs = await self.jr.list_next_jobs(job.id)

        next_nodes = []

        for n_job in next_jobs:
            c = await self.cr.get_by_id(n_job.component_id)
            next_nodes.append(
                TaskNode(
                    component_id=c.id,
                    public_key=c.public_key,
                    url=c.url,
                    is_local=c.id == self.component.id,
                )
            )

        # resource id produced
        resource = await self.rr.get_by_job_id(job.id)

        # task to execute
        task = Task(
            artifact_id=artifact.id,
            project_id=artifact.project_id,
            job_id=job.id,
            iteration=job.iteration,
            step=scheduler_job.step,
            task_resources=task_resources,
            next_nodes=next_nodes,
            resource_id=resource.id,
        )

        return task

    async def task_started(self, job_id: str) -> None:
        LOGGER.info(f"job={job_id}: task started")

        try:
            job = await self.jr.get_by_id(job_id)
            job = await self.jr.start_execution(job)

            await self.ar.update_status(job.artifact_id, ArtifactJobStatus.RUNNING)

        except NoResultFound:
            raise ValueError(f"component={self.component.id}: job={job_id} does not exists")

    async def task_completed(self, job_id: str) -> None:
        LOGGER.info(f"job={job_id}: task completed")

        try:
            job = await self.jr.get_by_id(job_id)
            job = await self.jr.complete_execution(job)

            await self.jr.unlock_job(job)
            await self.check(job.artifact_id)

        except NoResultFound:
            raise ValueError(f"component={self.component.id}: job={job_id} does not exists")

    async def task_failed(self, error: TaskError) -> Resource:
        LOGGER.error(f"job={error.job_id}: task failed: {error.message}")
        # TODO: where do we want to save the error message?

        try:
            job = await self.jr.get_by_id(error.job_id)

            await self.jr.failed_execution(job)

            resource = await self.store_resource(error.job_id, True)

            job = await self.jr.get_by_id(error.job_id)

            # mark artifact as error
            LOGGER.error(f"job={error.job_id}: aggregation failed for artifact={job.artifact_id}")
            await self.ar.update_status(job.artifact_id, ArtifactJobStatus.ERROR)

            async with aiofiles.open(resource.path, "w") as out_file:
                content = json.dumps(error.dict())
                await out_file.write(content)

            return resource

        except NoResultFound:
            raise ValueError(f"component={self.component.id}: job={error.job_id} does not exists")

    async def metrics(self, metrics: Metrics) -> None:
        artifact = await self.ar.get_artifact(metrics.artifact_id)

        if artifact is None:
            raise ValueError(f"artifact={metrics.artifact_id} assigned to metrics not found")

        path = await self.ar.storage_location(artifact.id, f"metrics_{metrics.source}.json")

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(metrics.dict())
            await f.write(content)
