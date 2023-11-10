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
from ferdelance.tasks.tasks import TaskError
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
            j = await self.jr.create_job(
                artifact.id,
                job,
            )
            job_map[job.id] = j

        # insert unlocks in database
        for job in jobs:
            j = job_map[job.id]
            unlocks = [job_map[i] for i in job.locks]

            await self.jr.add_locks(j, unlocks)

        # set first job to scheduled
        if iteration == 0:
            job = job_map[0]
            LOGGER.info(f"component={self.component.id}: scheduling initialization job={job.id}")
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

        resource = await self.rr.create_resource(
            job_id,
            job.artifact_id,
            self.component.id,
            job.iteration,
            is_error=is_error,
        )

        return resource

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


"""
    # TODO: this should be internal to a task and referred to the ENVIRONMENT type
    async def store_context(self, context: TaskParameters, init: bool = False) -> None:
        job = await self.jr.get_by_id(context.job_id)

        if job.component_id != self.component.id:
            raise ValueError("Invalid component!")

        if init:
            path_init = await self.ar.storage_location(context.artifact_id, "context.data.init.json")

            # initialization context will be used at the end
            async with aiofiles.open(path_init, "w") as f:
                json.dump(context.data, f)

        # current content, same as init
        path = await self.ar.storage_location(context.artifact_id, "context.data.json")
        async with aiofiles.open(path, "w") as f:
            json.dump(context.data, f)

        await self.jr.complete_execution(job)

        # TODO: check aggregation?

    # TODO: this should be internal to a task and referred to the ENVIRONMENT type
    async def load_context(self, artifact_id: str, job_id: str) -> TaskParameters:
        # fetch task
        job = await self.jr.get_by_id(job_id)

        if job.component_id != self.component.id:
            raise ValueError("Invalid component!")

        if job.artifact_id != artifact_id:
            raise ValueError("Invalid artifact!")

        # check that the artifact exists
        await self.ar.get_artifact(artifact_id)
        artifact = await self.ar.load(artifact_id)

        path = await self.ar.storage_location(artifact_id, "context.json")
        if aiofiles.ospath.exists(path):
            async with aiofiles.open(path, "w") as f:
                content = await f.read()
                data = json.loads(content)
        else:
            data = dict()

        context = TaskParameters(
            artifact_id=artifact_id,
            artifact=artifact,
            job_id=job.id,
            iteration=job.iteration,
            data=data,
        )

        await self.jr.start_execution(job)

        return context

    async def _task_start(self, job_id: str) -> TaskParameters:
        try:
            LOGGER.info(f"component={self.component.id}: start job={job_id}")

            # fetch task
            job = await self.jr.get_by_id(job_id)
            artifact_id = job.artifact_id

            artifact_db: ServerArtifact = await self.ar.get_artifact(artifact_id)

            artifact_path = artifact_db.path

            # check that the artifact exists on disk
            if not os.path.exists(artifact_path):
                LOGGER.warning(
                    f"component={self.component.id}: artifact={artifact_id} does not exist with path={artifact_path}"
                )
                raise ArtifactDoesNotExists()

            async with aiofiles.open(artifact_path, "r") as f:
                data = await f.read()
                artifact = Artifact(**json.loads(data))

            # discriminate based on job type
            if job.is_aggregation:
                if ArtifactJobStatus[artifact_db.status] != ArtifactJobStatus.AGGREGATING:
                    LOGGER.error(
                        f"component={self.component.id}: aggregating"
                        f"task job={job_id} for artifact={artifact_id} is in an unexpected state={artifact_db.status}"
                    )
                    raise ValueError(f"Wrong status for job={job_id}")

                resources: list[Resource] = await self.rr.list_resources_by_artifact_id(
                    artifact_id, artifact_db.iteration
                )

                content: list[str] = [r.id for r in resources]

            elif job.is_estimation or job.is_model:
                if ArtifactJobStatus[artifact_db.status] == ArtifactJobStatus.SCHEDULED:
                    await self.ar.update_status(artifact_id, ArtifactJobStatus.TRAINING)

                elif ArtifactJobStatus[artifact_db.status] == ArtifactJobStatus.TRAINING:
                    LOGGER.warning(f"component={self.component.id}: requested task already in training status?")

                else:
                    LOGGER.error(
                        f"component={self.component.id}: "
                        f"task job={job_id} for artifact={artifact_id} is in an unexpected state={artifact_db.status}"
                    )
                    raise ValueError(f"Wrong status for job={job_id}")

                content: list[str] = await self.dsr.list_hash_by_client_and_project(
                    self.component.id, artifact.project_id
                )

                if len(content) == 0:
                    LOGGER.warning(
                        f"component={self.component.id}: "
                        f"task with job={job_id} has no datasources with artifact={artifact_id}"
                    )
                    raise ValueError("TaskDoesNotExists")

            else:
                LOGGER.error(f"component={self.component.id}: invalid type for job={job.id}")
                raise ValueError(f"Invalid type for job={job_id}")

            # TODO: for complex training, filter based on artifact.load field

            await self.jr.start_execution(job)

            return TaskParameters(
                artifact=artifact,
                job_id=job.id,
                content_ids=content,
                iteration=job.iteration,
            )

        except NoResultFound:
            LOGGER.warning(f"component={self.component.id}: task with job={job_id} does not exists")
            raise ValueError("TaskDoesNotExists")

    async def check_aggregation(
        self,
        job: Job,
        artifact: Artifact,
        context: SchedulerContext,
    ) -> bool:
        # aggregations checks for
        context.aggregations = await self.jr.count_jobs_by_artifact_id(
            artifact.id,
            job.iteration,
            True,
        )
        context.aggregations_failed = await self.jr.count_jobs_by_artifact_status(
            artifact.id,
            JobStatus.ERROR,
            job.iteration,
            True,
        )

        context.job_total = await self.jr.count_jobs_by_artifact_id(
            artifact.id,
            job.iteration,
            False,
        )
        context.job_completed = await self.jr.count_jobs_by_artifact_status(
            artifact.id,
            JobStatus.COMPLETED,
            job.iteration,
            False,
        )
        context.job_failed = await self.jr.count_jobs_by_artifact_status(
            artifact.id,
            JobStatus.ERROR,
            job.iteration,
            False,
        )

        if artifact.has_plan():
            await artifact.get_plan().pre_aggregation_hook(context)

        if context.has_failed():
            LOGGER.error(f"artifact={artifact.id}: aggregation failed: {context.job_failed} jobs have error")
            await self.ar.update_status(artifact.id, ArtifactJobStatus.ERROR, context.current_iteration)
            return False

        if not context.completed():
            LOGGER.info(
                f"artifact={artifact.id}: "
                f"{context.job_completed} / {context.job_total} completed job(s) waiting for others",
            )
            return False

        if context.has_aggregations_failed():
            LOGGER.warning(f"artifact={artifact.id}: ")
            return False

        return True

    async def check_next_iteration(
        self,
        artifact: Artifact,
        context: SchedulerContext,
    ):
        # check for next iteration
        if artifact.has_plan():
            await artifact.get_plan().post_aggregation_hook(context)

        if context.schedule_next_iteration:
            # schedule next iteration
            LOGGER.info(f"artifact={artifact.id}: scheduling next iteration={context.next_iteration}")

            await self.ar.update_status(artifact.id, ArtifactJobStatus.SCHEDULED, context.next_iteration)
            await self.schedule_tasks_for_iteration(artifact, context.next_iteration)
            return

        # mark artifact as completed
        LOGGER.info(f"artifact={artifact.id}: artifact completed ")
        await self.ar.update_status(artifact.id, ArtifactJobStatus.COMPLETED, context.next_iteration)
        return

    async def _check(self, job_id: str, start_function) -> None:
        job, artifact, context = await self._context(job_id)

        if job.is_aggregation:
            if job.status == JobStatus.ERROR:
                LOGGER.warn(f"artifact={artifact.id}: aggregation failed")
                await self.ar.update_status(artifact.id, ArtifactJobStatus.ERROR, context.current_iteration)
                return

            await self.check_next_iteration(artifact, context)

        elif job.is_estimation or job.is_model:
            aggregate = await self.check_aggregation(job, artifact, context)

            if aggregate:
                LOGGER.info(
                    f"artifact={artifact.id}: " f"all {context.job_total} job(s) completed, starting aggregation"
                )

                await self.start_aggregation(artifact.id, job.is_model, job.is_estimation, start_function)

    async def start_aggregation(
        self,
        artifact_id: str,
        is_model: bool,
        is_estimation: bool,
        backend: Backend | None = None,
    ) -> Job:
        if not backend:
            backend = get_jobs_backend()

        artifact = await self.ar.get_status(artifact_id)

        try:
            # schedule an aggregation
            worker = await self.cr.get_self_component()

            agg_job: Job = await self.jr.create_job(
                artifact_id,
                worker.id,
                is_model=is_model,
                is_estimation=is_estimation,
                is_aggregation=True,
                iteration=artifact.iteration,
            )

            backend.start_aggregation(
                artifact_id=artifact_id,
                job_id=agg_job.id,
                component_id=worker.id,
                private_key=self.private_key,
                node_url=config_manager.get().url_extern(),
                node_public_key=self.node_public_key,
            )

            await self.ar.update_status(artifact_id, ArtifactJobStatus.AGGREGATING)

            return agg_job

        except ValueError as e:
            LOGGER.error(f"artifact={artifact_id}: aggregation impossible: no worker available")
            raise e

        except IntegrityError:
            LOGGER.warning(f"artifact={artifact_id}: trying to re-schedule an already existing aggregation job")
            await self.session.rollback()
            return await self.jr.get_by_artifact(
                artifact_id,
                self.component.id,
                artifact.iteration,
            )

        except NoResultFound:
            raise ValueError(f"artifact={artifact_id} not found")

        except Exception as e:
            LOGGER.exception(e)
            raise e
"""
