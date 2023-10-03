from typing import Callable

from ferdelance.config import config_manager
from ferdelance.database.repositories import (
    AsyncSession,
    ArtifactRepository,
    ComponentRepository,
    DataSourceRepository,
    JobRepository,
    ProjectRepository,
    ResultRepository,
    Repository,
)
from ferdelance.exceptions import ArtifactDoesNotExists
from ferdelance.logging import get_logger
from ferdelance.node.services import SecurityService, ActionService
from ferdelance.schemas.artifacts import Artifact, ArtifactStatus
from ferdelance.schemas.components import Component
from ferdelance.schemas.context import TaskContext, JobFromContext
from ferdelance.schemas.database import ServerArtifact, Result
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.tasks import TaskParameters, TaskArguments, TaskError
from ferdelance.schemas.updates import UpdateData
from ferdelance.shared.status import JobStatus, ArtifactJobStatus
from ferdelance.tasks.backends import get_jobs_backend

from sqlalchemy.exc import NoResultFound, IntegrityError

import aiofiles
import json
import os

LOGGER = get_logger(__name__)


class JobManagementService(Repository):
    def __init__(self, session: AsyncSession, component: Component) -> None:
        super().__init__(session)

        self.component: Component = component
        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.ax: ActionService = ActionService(self.session)
        self.cr: ComponentRepository = ComponentRepository(session)
        self.dsr: DataSourceRepository = DataSourceRepository(session)
        self.jr: JobRepository = JobRepository(session)
        self.pr: ProjectRepository = ProjectRepository(session)
        self.rr: ResultRepository = ResultRepository(session)

        self.ss: SecurityService = SecurityService()

    async def update(self) -> UpdateData:
        await self.cr.create_event(self.component.id, "update")
        next_action = await self.ax.next(self.component)

        LOGGER.debug(f"component={self.component.id}: update action={next_action.action}")

        await self.cr.create_event(self.component.id, f"action:{next_action.action}")

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
        project = await self.pr.get_by_id(artifact.project_id)
        datasources_ids = await self.pr.list_datasources_ids(project.token)

        workers: list[Component] = []
        for datasource_id in datasources_ids:
            worker: Component = await self.dsr.get_client_by_datasource_id(datasource_id)
            workers.append(worker)

        LOGGER.info(f"artifact={artifact.id}: creating jobs with {len(workers)} worker(s) for it={iteration}")

        context = TaskContext(
            artifact=artifact,
            initiator=self.component,
            workers=workers,
            current_iteration=iteration,
        )

        jobs: list[JobFromContext] = artifact.get_plan().get_jobs(context)

        LOGGER.info(f"artifact={artifact.id}: planned to schedule {len(jobs)} job(s) for it={iteration}")

        job_map: dict[int, Job] = dict()

        # insert jobs in database
        for job in jobs:
            j = await self.jr.add_job(
                job.artifact.id,
                job.worker.id,
                is_model=job.artifact.is_model(),
                is_estimation=job.artifact.is_estimation(),
                iteration=job.iteration,
                counter=job.counter,
                work_type=job.work_type,
            )
            job_map[job.id] = j

        # insert unlocks in database
        for job in jobs:
            j = job_map[job.id]
            unlocks = [job_map[i] for i in job.unlocks]

            await self.jr.add_unlocks(j, unlocks)

        # set first (init) job to scheduled
        if iteration == 0:
            job = job_map[0]
            LOGGER.info(f"component={self.component.id}: starting initialization job={job}")
            await self.jr.schedule_job(job)

            get_jobs_backend().start_init()

    async def task_start(self, job_id: str) -> TaskParameters:
        try:
            await self.cr.create_event(self.component.id, f"start task {job_id}")

            # fetch task
            job = await self.jr.get_by_id(job_id)
            artifact_id = job.artifact_id

            artifact_db: ServerArtifact = await self.ar.get_artifact(artifact_id)

            artifact_path = artifact_db.path

            # check that the artifact exists on idsk
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

                results: list[Result] = await self.rr.list_results_by_artifact_id(artifact_id, artifact_db.iteration)

                content: list[str] = [r.id for r in results]

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

    async def _create_result(self, job_id: str) -> Result:
        job = await self.jr.get_by_id(job_id)
        artifact_id = job.artifact_id

        # simple check
        await self.ar.get_artifact(artifact_id)

        result = await self.rr.create_result(
            job_id,
            job.artifact_id,
            self.component.id,
            job.iteration,
            job.is_estimation,
            job.is_model,
            job.is_aggregation,
        )

        return result

    async def task_completed(self, job_id: str) -> Result:
        LOGGER.info(f"component={self.component.id}: job={job_id} completed")

        try:
            result = await self._create_result(job_id)

            await self.jr.mark_completed(job_id, self.component.id)

            return result

        except NoResultFound:
            raise ValueError(f"component={self.component.id}: job={job_id} does not exists")

    async def get_result(self, result_id: str) -> Result:
        """
        :raise:
            NoResultFound if there is no result on the disk.
        """
        result: Result = await self.rr.get_by_id(result_id)

        if not os.path.exists(result.path):
            raise NoResultFound()

        return result

    async def check_aggregation(
        self,
        job: Job,
        artifact: Artifact,
        context: TaskContext,
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

    async def _context(self, job_id: str) -> tuple[Job, Artifact, TaskContext]:
        job: Job = await self.jr.get_by_id(job_id)
        it = job.iteration

        artifact: Artifact = await self.ar.load(job.artifact_id)

        context = TaskContext(
            artifact_id=artifact.id,
            current_iteration=it,
            next_iteration=it + 1,
        )

        return job, artifact, context

    async def check_next_iteration(
        self,
        artifact: Artifact,
        context: TaskContext,
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

    async def check(self, job_id: str, start_function: Callable[[TaskArguments], None] | None = None) -> None:
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
        start_function: Callable[[TaskArguments], None] | None = None,
    ) -> Job:
        if not start_function:
            backend = get_jobs_backend()
            start_function = backend.start_aggregation

        artifact = await self.ar.get_status(artifact_id)

        try:
            # schedule an aggregation
            worker = await self.cr.get_self_component()

            agg_job: Job = await self.jr.add_job(
                artifact_id,
                worker.id,
                is_model=is_model,
                is_estimation=is_estimation,
                is_aggregation=True,
                iteration=artifact.iteration,
            )

            args = TaskArguments(
                component_id=worker.id,
                private_key=self.ss.get_private_key(),
                node_url=config_manager.get().url_extern(),
                node_public_key=self.ss.get_public_key(),
                datasources=list(),
                workdir=".",
                job_id=agg_job.id,
                artifact_id=artifact_id,
            )

            start_function(args)

            await self.ar.update_status(artifact_id, ArtifactJobStatus.AGGREGATING)

            return agg_job

        except ValueError as e:
            LOGGER.error(f"artifact={artifact_id}: aggregation impossile: no worker available")
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

    async def task_failed(self, error: TaskError) -> Result:
        LOGGER.error(f"job={error.job_id}: task failed: {error.message}")
        # TODO: where do we want to save the error message?

        try:
            await self.jr.mark_error(error.job_id, self.component.id)

            result = await self._create_result(error.job_id)

            job = await self.jr.get_by_id(error.job_id)

            if job.is_aggregation:
                # mark artifact as error
                LOGGER.error(f"job={error.job_id}: aggregation failed for artifact={job.artifact_id}")
                await self.ar.update_status(job.artifact_id, ArtifactJobStatus.ERROR)

            async with aiofiles.open(result.path, "w") as out_file:
                content = json.dumps(error.dict())
                await out_file.write(content)

            return result

        except NoResultFound:
            raise ValueError(f"component={self.component.id}: job={error.job_id} does not exists")

    async def metrics(self, metrics: Metrics) -> None:
        ar: ArtifactRepository = ArtifactRepository(self.session)

        artifact = await ar.get_artifact(metrics.artifact_id)

        if artifact is None:
            raise ValueError(f"artifact={metrics.artifact_id} assigned to metrics not found")

        path = await ar.storage_location(artifact.id, f"metrics_{metrics.source}.json")

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(metrics.dict())
            await f.write(content)
