from sched import scheduler
from typing import Sequence
from ferdelance.config.config import Configuration, config_manager
from ferdelance.const import TYPE_CLIENT
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
from ferdelance.node.services import ActionService
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import ServerArtifact, Resource
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.updates import UpdateData
from ferdelance.shared.status import JobStatus, ArtifactJobStatus
from ferdelance.tasks.tasks import Task, TaskError, TaskNode, TaskResource

from sqlalchemy.exc import NoResultFound
from uuid import uuid4

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
    ) -> None:
        super().__init__(session)

        self.self_component: Component = component  # this is the self-component!
        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.ax: ActionService = ActionService(self.session)
        self.cr: ComponentRepository = ComponentRepository(session)
        self.dsr: DataSourceRepository = DataSourceRepository(session)
        self.jr: JobRepository = JobRepository(session)
        self.pr: ProjectRepository = ProjectRepository(session)
        self.rr: ResourceRepository = ResourceRepository(session)

        self.config: Configuration = config_manager.get()

    async def update(self, component: Component) -> UpdateData:
        """This method is used to get an update for a client. Such update consists in the next action to execute and
        the parameters required to execute it. After this call, a client can request a task.

        Args:
            component (Component):
                The client component requesting an update.

        Returns:
            UpdateData:
                Container object with the next action to execute.
        """
        next_action = await self.ax.next(component)

        LOGGER.debug(f"component={component.id}: update action={next_action.action}")

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

            LOGGER.info(f"component={self.self_component.id}: submitted artifact={artifact.id}")

            await self.schedule_tasks(artifact)

            return artifact_db.get_status()
        except ValueError as e:
            raise e

    async def schedule_tasks(self, artifact: Artifact) -> None:
        """Schedules all the jobs for the given artifact in the current iteration.

        Args:
            artifact (Artifact):
                Artifact to schedule jobs for.
        """
        LOGGER.info(f"artifact={artifact.id}: collecting jobs to be scheduled")

        project = await self.pr.get_by_id(artifact.project_id)
        datasources_ids = await self.pr.list_datasources_ids(project.token)

        workers: list[Component] = []
        for datasource_id in datasources_ids:
            worker: Component = await self.dsr.get_node_by_datasource_id(datasource_id)
            workers.append(worker)

        LOGGER.info(f"artifact={artifact.id}: creating jobs with {len(workers)} worker(s)")

        context = SchedulerContext(
            artifact_id=artifact.id,
            initiator=self.self_component,
            workers=workers,
        )

        jobs: Sequence[SchedulerJob] = artifact.jobs(context)

        LOGGER.info(f"artifact={artifact.id}: planned to schedule {len(jobs)} job(s)")

        job_map: dict[int, Job] = dict()

        # insert jobs in database
        for job in jobs:
            job_id = str(uuid4())

            resource = await self.rr.create_resource(
                job_id,
                artifact.id,
                job.worker.id,
                job.iteration,
            )

            job_db = await self.jr.create_job(
                artifact.id,
                job,
                resource.id,
                job_id=job_id,
            )
            job_map[job.id] = job_db

        # insert locks in database
        for job in jobs:
            job_db = job_map[job.id]
            unlocks = [job_map[i] for i in job.locks]

            await self.jr.add_locks(job_db, unlocks)

    async def next_task_for_component(self, component_id: str) -> str | None:
        jobs = await self.jr.list_scheduled_jobs_for_component(component_id)

        LOGGER.info(f"component={self.self_component.id}: found {len(jobs)} job(s) for component={component_id}")

        if len(jobs) < 1:
            return None

        return jobs[0].id

    async def get_scheduled_jobs(self, artifact_id: str) -> list[Job]:
        return await self.jr.list_jobs_by_artifact_id(artifact_id)

    async def check(self, artifact_id: str) -> None:
        LOGGER.info(f"component={self.self_component.id}: checking changes for artifact={artifact_id}")

        jobs_ready = await self.jr.list_unlocked_jobs_by_artifact_id(artifact_id)

        artifact = await self.ar.get_artifact(artifact_id)
        it = artifact.iteration

        jobs_to_start = 0

        for job in jobs_ready:
            if job.status == JobStatus.WAITING:
                it = job.iteration
                await self.jr.schedule_job(job)
                jobs_to_start += 1

        if jobs_to_start > 0:
            await self.ar.update_status(artifact_id, ArtifactJobStatus.RUNNING, it)
            LOGGER.info(
                f"component={self.self_component.id}: updated artifact={artifact_id} "
                f"to status={ArtifactJobStatus.RUNNING} it={it} with {jobs_to_start} job(s) to start"
            )

        jobs_ready = await self.jr.list_jobs_by_artifact_id(artifact_id)

        all_jobs_completed = True

        for job in jobs_ready:
            if job.status != JobStatus.COMPLETED:
                all_jobs_completed = False
                break

        if all_jobs_completed:
            await self.ar.update_status(artifact_id, ArtifactJobStatus.COMPLETED, it)
            LOGGER.info(
                f"component={self.self_component.id}: updated artifact={artifact_id} to status={ArtifactJobStatus.COMPLETED} it={it}"
            )

        LOGGER.info(f"component={self.self_component.id}: checking done for artifact={artifact_id}")

    async def get_task_by_job_id(self, job_id: str) -> Task:
        LOGGER.info(f"component={self.self_component.id}: getting task for job={job_id}")

        # TODO: add checks if who is downloading the job is allowed to do so
        # FIXME: this is bad written: multiple queries can be aggregate together

        job = await self.jr.get_by_id(job_id)
        artifact_id: str = job.artifact_id
        it = job.iteration

        worker = await self.cr.get_by_id(job.component_id)

        scheduler_job = await self.jr.load(job)
        artifact = await self.ar.load(artifact_id)

        art_db = await self.ar.get_status(artifact_id)
        if art_db == ArtifactJobStatus.SCHEDULED:
            await self.ar.update_status(artifact_id, ArtifactJobStatus.RUNNING, it)
            LOGGER.info(
                f"component={self.self_component.id}: updated artifact={artifact_id} "
                f"to status={ArtifactJobStatus.RUNNING} it={it}"
            )

        project = await self.pr.get_by_id(artifact.project_id)

        # collect required resources
        prev_jobs = await self.jr.list_previous_jobs(job.id)

        task_resources = []

        for p_job in prev_jobs:
            r = await self.rr.get_by_job_id(p_job.id)

            if worker.id == self.self_component.id:
                # job for scheduler
                available_locally = True
                component_id = self.self_component.id
                public_key = self.self_component.public_key
                path = str(r.path)
                url = self.config.url_localhost()

            elif worker.type_name == TYPE_CLIENT:
                # job for clients
                available_locally = False
                component_id = self.self_component.id
                public_key = self.self_component.public_key
                path = None
                url = self.config.url_extern()

            else:
                # job for nodes
                c = await self.cr.get_by_id(p_job.component_id)

                # TODO: This depends if resources need to be collected or not...
                available_locally = True
                component_id = c.id
                public_key = c.public_key
                path = str(r.path)
                url = c.url

            task_resources.append(
                TaskResource(
                    # resource
                    resource_id=r.id,
                    artifact_id=job.artifact_id,
                    iteration=job.iteration,
                    job_id=p_job.id,
                    # who has the resource
                    component_id=component_id,
                    component_public_key=public_key,
                    component_url=url,
                    # local data
                    available_locally=available_locally,
                    local_path=path,
                )
            )

        # collect next resources
        next_jobs = await self.jr.list_next_jobs(job.id)

        next_nodes = []

        for n_job in next_jobs:
            c = await self.cr.get_by_id(n_job.component_id)
            next_nodes.append(
                TaskNode(
                    # target_data
                    target_id=c.id,
                    target_public_key=c.public_key,
                    target_url=c.url,
                    # use scheduler as proxy since target is a CLIENT
                    use_scheduler_as_proxy=c.type_name == TYPE_CLIENT,
                )
            )

        # resource id produced
        resource = await self.rr.get_by_job_id(job.id)

        # task to execute
        task = Task(
            project_token=project.token,
            artifact_id=artifact.id,
            job_id=job.id,
            iteration=job.iteration,
            step=scheduler_job.step,
            required_resources=task_resources,
            next_nodes=next_nodes,
            produced_resource_id=resource.id,
        )

        await self.task_started(job_id)

        return task

    async def task_started(self, job_id: str) -> None:
        LOGGER.info(f"job={job_id}: task starting")

        try:
            job = await self.jr.get_by_id(job_id)

            if job.status != JobStatus.SCHEDULED:
                LOGGER.warning(
                    f"component={self.self_component.id}: job={job_id} in status={job.status} "
                    f"while expected status={JobStatus.SCHEDULED}"
                )
                return

            job = await self.jr.start_execution(job)

            await self.ar.update_status(job.artifact_id, ArtifactJobStatus.RUNNING)

        except Exception as e:
            LOGGER.exception(e)
            raise ValueError(f"component={self.self_component.id}: job={job_id} does not exists")

    async def task_completed(self, job_id: str) -> None:
        LOGGER.info(f"job={job_id}: task completed")

        try:
            job = await self.jr.get_by_id(job_id)
            job = await self.jr.complete_execution(job)

            await self.rr.mark_as_done(job.id)
            await self.jr.unlock_job(job)

        except NoResultFound:
            raise ValueError(f"component={self.self_component.id}: job={job_id} does not exists")

    async def task_failed(self, error: TaskError) -> Resource:
        LOGGER.error(f"job={error.job_id}: task failed: {error.message}")
        # TODO: where do we want to save the error message?

        try:
            job = await self.jr.get_by_id(error.job_id)

            await self.jr.failed_execution(job)

            resource = await self.rr.mark_as_error(job_id=error.job_id)

            job = await self.jr.get_by_id(error.job_id)

            # mark artifact as error
            LOGGER.error(f"job={error.job_id}: aggregation failed for artifact={job.artifact_id}")
            await self.ar.update_status(job.artifact_id, ArtifactJobStatus.ERROR)

            async with aiofiles.open(resource.path, "w") as out_file:
                content = json.dumps(error.dict(), indent=True)
                await out_file.write(content)

            return resource

        except NoResultFound:
            raise ValueError(f"component={self.self_component.id}: job={error.job_id} does not exists")

    async def metrics(self, metrics: Metrics) -> None:
        artifact = await self.ar.get_artifact(metrics.artifact_id)

        if artifact is None:
            raise ValueError(f"artifact={metrics.artifact_id} assigned to metrics not found")

        path = await self.ar.storage_location(artifact.id, f"metrics_{metrics.source}.json")

        LOGGER.info(f"component={self.self_component.id}: saving metrics for job={metrics.job_id}")

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(metrics.dict(), indent=True)
            await f.write(content)
