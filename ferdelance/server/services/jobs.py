from typing import Callable

from ferdelance.database.repositories import (
    AsyncSession,
    ArtifactRepository,
    DataSourceRepository,
    JobRepository,
    ResultRepository,
    ComponentRepository,
    ProjectRepository,
    Repository,
)
from ferdelance.schemas.artifacts import Artifact, ArtifactStatus
from ferdelance.schemas.components import Client
from ferdelance.schemas.context import AggregationContext
from ferdelance.schemas.database import ServerArtifact, Result
from ferdelance.schemas.errors import TaskError
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.tasks import TaskParameters, TaskArguments
from ferdelance.server.exceptions import ArtifactDoesNotExists
from ferdelance.shared.status import JobStatus, ArtifactJobStatus
from ferdelance.worker.backends import get_jobs_backend

from sqlalchemy.exc import NoResultFound, IntegrityError

import aiofiles
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


class JobManagementService(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.cr: ComponentRepository = ComponentRepository(session)
        self.dsr: DataSourceRepository = DataSourceRepository(session)
        self.jr: JobRepository = JobRepository(session)
        self.pr: ProjectRepository = ProjectRepository(session)
        self.rr: ResultRepository = ResultRepository(session)

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
            # TODO: manage for estimates

            artifact_db: ServerArtifact = await self.ar.create_artifact(artifact)

            artifact.id = artifact_db.id

            await self.schedule_tasks_for_clients(artifact, 0)

            return artifact_db.get_status()
        except ValueError as e:
            raise e

    async def schedule_tasks_for_clients(self, artifact: Artifact, iteration: int) -> None:
        project = await self.pr.get_by_id(artifact.project_id)
        datasources_ids = await self.pr.list_datasources_ids(project.token)

        LOGGER.info(f"artifact_id={artifact.id}: scheduling {len(datasources_ids)} job(s) for iteration #{artifact}")

        for datasource_id in datasources_ids:
            client: Client = await self.dsr.get_client_by_datasource_id(datasource_id)

            await self.jr.schedule_job(
                artifact.id,
                client.id,
                is_model=artifact.is_model(),
                is_estimation=artifact.is_estimation(),
                iteration=iteration,
            )

    async def client_task_start(self, job_id: str, client_id: str) -> TaskParameters:
        try:
            job = await self.jr.get_by_id(job_id)
            artifact_id = job.artifact_id

            artifact_db: ServerArtifact = await self.ar.get_artifact(artifact_id)

            artifact_path = artifact_db.path

            if not os.path.exists(artifact_path):
                LOGGER.warning(
                    f"client_id={client_id}: artifact_id={artifact_id} does not exist with path={artifact_path}"
                )
                raise ArtifactDoesNotExists()

            if ArtifactJobStatus[artifact_db.status] == ArtifactJobStatus.SCHEDULED:
                await self.ar.update_status(artifact_id, ArtifactJobStatus.TRAINING)
            elif ArtifactJobStatus[artifact_db.status] == ArtifactJobStatus.TRAINING:
                pass  # already in correct state
            else:
                LOGGER.error(
                    f"client_id={client_id}: "
                    f"task job_id={job_id} for artifact_id={artifact_id} is in an unexpected state={artifact_db.status}"
                )
                raise ValueError(f"Wrong status for job_id={job_id}")

            async with aiofiles.open(artifact_path, "r") as f:
                data = await f.read()
                artifact = Artifact(**json.loads(data))

            hashes = await self.dsr.list_hash_by_client_and_project(client_id, artifact.project_id)

            if len(hashes) == 0:
                LOGGER.warning(
                    f"client_id={client_id}: "
                    f"task with job_id={job_id} has no datasources with artifact_id={artifact_id}"
                )
                raise ValueError("TaskDoesNotExists")

            # TODO: for complex training, filter based on artifact.load field

            await self.jr.start_execution(job)

            return TaskParameters(artifact=artifact, job_id=job.id, content_ids=hashes)

        except NoResultFound:
            LOGGER.warning(f"client_id={client_id}: task with job_id={job_id} does not exists")
            raise ValueError("TaskDoesNotExists")

    async def check_for_aggregation(self, result: Result) -> bool:
        artifact_id = result.artifact_id

        try:
            job = await self.jr.get_by_id(result.job_id)
            it = job.iteration

            artifact: Artifact = await self.ar.load(artifact_id)
            context = AggregationContext(
                artifact_id=artifact_id,
                current_iteration=it,
                next_iteration=it + 1,
            )

            context.job_total = await self.jr.count_jobs_by_artifact_id(artifact_id, it)
            context.job_completed = await self.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.COMPLETED, it)
            context.job_failed = await self.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.ERROR, it)

            if artifact.has_plan():
                await artifact.get_plan().pre_aggregation_hook(context)

            if context.has_failed():
                LOGGER.error(
                    f"artifact_id={result.artifact_id}: " f"aggregation impossile: {context.job_failed} jobs have error"
                )
                return False

            if not context.completed():
                LOGGER.info(
                    f"artifact_id={result.artifact_id}: "
                    f"aggregation impossile: {context.job_completed} / {context.job_total} completed job(s)"
                )
                return False

            LOGGER.info(
                f"artifact_id={result.artifact_id}: " f"all {context.job_total} job(s) completed, starting aggregation"
            )

            return True

        except NoResultFound:
            raise ValueError(f"artifact_id={artifact_id} not found")

        except Exception as e:
            LOGGER.exception(e)
            raise e

    async def start_aggregation(self, result: Result) -> Job:
        backend = get_jobs_backend()
        job = await self._start_aggregation(result, backend.start_aggregation)
        return job

    async def _start_aggregation(self, result: Result, start_function: Callable[[TaskArguments], None]) -> Job:
        artifact_id = result.artifact_id

        try:
            token = await self.cr.get_token_for_workers()

            if token is None:
                raise ValueError("No worker available")

            artifact = await self.ar.get_status(artifact_id)

            # schedule an aggregation
            worker_id = await self.cr.get_component_id_by_token(token)

            job: Job = await self.jr.schedule_job(
                artifact_id,
                worker_id,
                is_model=result.is_model,
                is_estimation=result.is_estimation,
                is_aggregation=True,
                iteration=artifact.iteration,
            )

            args = TaskArguments(
                private_key_location="",  # TODO: fix these?
                server_url="",
                server_public_key="",
                token=token,
                datasources=list(),
                workdir=".",
                job_id=job.id,
                artifact_id=artifact_id,
            )

            start_function(args)

            await self.ar.update_status(artifact_id, ArtifactJobStatus.AGGREGATING)

            return job

        except ValueError as e:
            LOGGER.error(f"artifact_id={artifact_id}: aggregation impossile: no worker available")
            raise e

        except IntegrityError as e:
            LOGGER.warning(f"artifact_id={artifact_id}: trying to re-schedule an already existing aggregation job")
            await self.session.rollback()
            raise e

        except NoResultFound:
            raise ValueError(f"artifact_id={artifact_id} not found")

        except Exception as e:
            LOGGER.exception(e)
            raise e

    async def worker_task_start(self, job_id: str, client_id: str) -> TaskParameters:
        try:
            job = await self.jr.get_by_id(job_id)
            artifact_id = job.artifact_id

            artifact_db: ServerArtifact = await self.ar.get_artifact(artifact_id)

            if ArtifactJobStatus[artifact_db.status] != ArtifactJobStatus.AGGREGATING:
                raise ValueError(f"Wrong status for job_id={job_id}")

            async with aiofiles.open(artifact_db.path, "r") as f:
                data = await f.read()
                artifact = Artifact(**json.loads(data))

            await self.jr.start_execution(job)

            results: list[Result] = await self.rr.list_results_by_artifact_id(artifact_id, artifact_db.iteration)

            return TaskParameters(
                artifact=artifact,
                job_id=job_id,
                content_ids=[r.id for r in results],
            )

        except NoResultFound:
            LOGGER.warning(f"client_id={client_id}: task with job_id={job_id} does not exists")
            raise ValueError("TaskDoesNotExists")

    async def create_result(
        self, job_id: str, producer_id: str, is_aggregation: bool = False, is_error: bool = False
    ) -> Result:
        LOGGER.info(f"component_id={producer_id}: creating results for job_id={job_id}")
        try:
            job = await self.jr.get_by_id(job_id)
            artifact_id = job.artifact_id

            # simple check
            await self.ar.get_artifact(artifact_id)

            artifact: Artifact = await self.ar.load(artifact_id)

            result = await self.rr.create_result(
                job_id=job_id,
                artifact_id=artifact_id,
                producer_id=producer_id,
                iteration=job.iteration,
                is_estimation=artifact.is_estimation(),
                is_model=artifact.is_model(),
                is_aggregation=is_aggregation,
                is_error=is_error,
            )

            return result

        except NoResultFound:
            raise ValueError(f"component_id={producer_id}: job_id={job_id} does not exists")

    async def client_task_completed(self, job_id: str, client_id: str) -> None:
        LOGGER.info(f"job_id={job_id}: task completed")

        await self.jr.mark_completed(job_id, client_id)

    async def client_task_failed(self, error: TaskError, client_id: str) -> None:
        LOGGER.info(f"job_id={error.job_id}: task completed")

        await self.jr.mark_error(error.job_id, client_id)

    async def aggregation_completed(self, job_id: str, worker_id: str) -> None:
        LOGGER.info(f"job_id={job_id}: aggregation completed")

        await self.jr.mark_completed(job_id, worker_id)

    async def check_for_iteration(self, job_id: str) -> None:
        # check for next iteration
        job = await self.jr.get_by_id(job_id)
        artifact_id = job.artifact_id

        artifact: Artifact = await self.ar.load(artifact_id)
        context = AggregationContext(
            artifact_id=artifact_id,
            current_iteration=job.iteration,
            next_iteration=job.iteration + 1,
        )

        if artifact.has_plan():
            await artifact.get_plan().post_aggregation_hook(context)

        if context.schedule_next_iteration:
            # schedule next iteration
            LOGGER.info(f"artifact_id={artifact_id}: scheduling next iteration #{context.next_iteration}")

            await self.ar.update_status(artifact_id, ArtifactJobStatus.SCHEDULED, context.next_iteration)
            await self.schedule_tasks_for_clients(artifact, context.next_iteration)

        else:
            # mark artifact as completed
            LOGGER.info(f"artifact_id={artifact_id}: artifact completed ")
            await self.ar.update_status(artifact_id, ArtifactJobStatus.COMPLETED, context.next_iteration)

    async def aggregation_failed(self, error: TaskError, worker_id: str) -> None:
        LOGGER.warning(f"job_id={error.job_id}: aggregation failed with error: {error.message}{error.stack_trace}")

        await self.jr.mark_error(error.job_id, worker_id)

        job = await self.jr.get_by_id(error.job_id)

        # mark artifact as error
        await self.ar.update_status(job.artifact_id, ArtifactJobStatus.ERROR)
