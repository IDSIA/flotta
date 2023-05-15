from ferdelance.database.repositories import (
    AsyncSession,
    ArtifactRepository,
    DataSourceRepository,
    JobRepository,
    ResultRepository,
    ComponentRepository,
    ProjectRepository,
    Repository,
    AsyncSession,
)

from ferdelance.schemas.artifacts import Artifact, ArtifactStatus
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.components import Client
from ferdelance.schemas.context import AggregationContext
from ferdelance.schemas.database import ServerArtifact, Result
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.worker import WorkerTask
from ferdelance.server.exceptions import ArtifactDoesNotExists, TaskDoesNotExists
from ferdelance.shared.status import JobStatus, ArtifactJobStatus
from ferdelance.worker.tasks.aggregation import aggregation

from sqlalchemy.exc import NoResultFound, IntegrityError
from celery.result import AsyncResult

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

    async def submit_artifact(self, artifact: Artifact, iteration=0) -> ArtifactStatus:
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
            ArtifactStatus: _description_
        """
        try:
            # TODO: maybe split artifact for each client on submit?

            # TODO: manage for estimates

            artifact_db: ServerArtifact = await self.ar.create_artifact(artifact)

            project = await self.pr.get_by_id(artifact.project_id)
            datasources_ids = await self.pr.list_datasources_ids(project.token)

            for datasource_id in datasources_ids:
                client: Client = await self.dsr.get_client_by_datasource_id(datasource_id)

                await self.jr.schedule_job(
                    artifact_db.artifact_id,
                    client.client_id,
                    is_model=artifact.is_model(),
                    is_estimation=artifact.is_estimation(),
                )

            return artifact_db.get_status()
        except ValueError as e:
            raise e

    async def get_artifact(self, artifact_id: str) -> Artifact:
        return await self.ar.load(artifact_id)

    async def client_task_start(self, job_id: str, client_id: str) -> ClientTask:
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
                    f"client_id={client_id}: task job_id={job_id} for artifact_id={artifact_id} is in an unexpected state={artifact_db.status}"
                )
                raise ValueError(f"Wrong status for job_id={job_id}")

            async with aiofiles.open(artifact_path, "r") as f:
                data = await f.read()
                artifact = Artifact(**json.loads(data))

            hashes = await self.dsr.list_hash_by_client_and_project(client_id, artifact.project_id)

            if len(hashes) == 0:
                LOGGER.warning(
                    f"client_id={client_id}: task with job_id={job_id} has no datasources with artifact_id={artifact_id}"
                )
                raise TaskDoesNotExists()

            # TODO: for complex training, filter based on artifact.load field

            await self.jr.start_execution(job)

            return ClientTask(artifact=artifact, job_id=job.job_id, datasource_hashes=hashes)

        except NoResultFound as _:
            LOGGER.warning(f"client_id={client_id}: task with job_id={job_id} does not exists")
            raise TaskDoesNotExists()

    async def worker_task_start(self, job_id: str, client_id: str) -> WorkerTask:
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

            return WorkerTask(
                artifact=artifact,
                job_id=job_id,
            )

        except NoResultFound:
            LOGGER.warning(f"client_id={client_id}: task with job_id={job_id} does not exists")
            raise TaskDoesNotExists()

    def _start_aggregation(self, token: str, job_id: str, result_ids: list[str], artifact_id: str) -> str:
        LOGGER.info(
            f"artifact_id={artifact_id}: started aggregation task with job_id={job_id} and {len(result_ids)} result(s)"
        )
        task: AsyncResult = aggregation.apply_async(
            args=[
                token,
                job_id,
                result_ids,
            ],
        )
        task_id = str(task.task_id)
        LOGGER.info(
            f"artifact_id={artifact_id}: scheduled task with job_id={job_id} celery_id={task_id} status={task.status}"
        )
        return task_id

    async def client_result_create(self, job_id: str, client_id: str) -> Result:
        LOGGER.info(f"client_id={client_id}: creating results for job_id={job_id}")

        job = await self.jr.get_by_id(job_id)
        artifact_id = job.artifact_id

        # simple check
        await self.ar.get_artifact(artifact_id)

        artifact: Artifact = await self.get_artifact(artifact_id)

        await self.jr.mark_completed(job_id, client_id)

        result = await self.rr.create_result(
            job_id=job_id,
            artifact_id=artifact_id,
            producer_id=client_id,
            iteration=job.iteration,
            is_estimation=artifact.is_estimation(),
            is_model=artifact.is_model(),
        )

        return result

    async def check_for_aggregation(self, result: Result) -> None:
        """This function is a check used to determine if starting the aggregation
        of an artifact or not. Conditions to start are: all jobs related to the
        current artifact (referenced in the argument result) is completed, and
        there are no errors.

        Args:
            result (Result):
                Result produced by a client.

        Raises:
            ValueError:
                If the artifact referenced by argument result does not exists.
        """
        artifact_id = result.artifact_id

        try:
            job = await self.jr.get_by_id(result.job_id)
            it = job.iteration

            artifact: Artifact = await self.get_artifact(artifact_id)
            context = AggregationContext(
                artifact_id=artifact_id,
                current_iteration=it,
                next_iteration=it + 1,
            )

            context.job_total = await self.jr.count_jobs_by_artifact_id(artifact_id, it)
            context.job_completed = await self.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.COMPLETED, it)
            context.job_failed = await self.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.ERROR, it)

            await artifact.get_plan().pre_aggregation_hook(context)

            if context.has_failed():
                LOGGER.error(
                    f"artifact_id={result.artifact_id}: cannot aggregate: {context.job_failed} jobs have error"
                )
                return

            if not context.completed():
                LOGGER.info(
                    f"artifact_id={result.artifact_id}: cannot aggregate: {context.job_completed} / {context.job_total} completed job(s)"
                )
                return

            LOGGER.info(
                f"artifact_id={result.artifact_id}: all {context.job_total} job(s) completed, starting aggregation"
            )

            token = await self.cr.get_token_for_workers()

            if token is None:
                LOGGER.error(f"artifact_id={result.artifact_id}: cannot aggregate: no worker available")
                return

            # schedule an aggregation
            worker_id = await self.cr.get_component_id_by_token(token)

            job: Job = await self.jr.schedule_job(
                artifact_id,
                worker_id,
                is_model=result.is_model,
                is_estimation=result.is_estimation,
                is_aggregation=True,
            )

            results: list[Result] = await self.rr.list_results_by_artifact_id(artifact_id)
            result_ids: list[str] = [m.result_id for m in results]

            task_id: str = self._start_aggregation(token, job.job_id, result_ids, artifact_id)

            await self.ar.update_status(artifact_id, ArtifactJobStatus.AGGREGATING)
            await self.jr.set_celery_id(job, task_id)

            LOGGER.info(f"artifact_id={artifact_id}: assigned celery_id={task_id} to job with job_id={job.job_id}")

        except IntegrityError:
            LOGGER.warning(f"artifact_id={artifact_id}: trying to re-schedule an already existing aggregation job")
            await self.session.rollback()
            return

        except NoResultFound:
            raise ValueError(f"artifact_id={artifact_id} not found")

        except Exception as e:
            LOGGER.exception(e)
            raise e

    async def worker_result_create(self, job_id: str, worker_id: str) -> Result:
        try:
            LOGGER.info(f"worker_id={worker_id}: creating aggregated result for job_id={job_id}")

            job = await self.jr.get_by_id(job_id)
            artifact_id = job.artifact_id

            # simple check
            await self.ar.get_artifact(artifact_id)

            artifact: Artifact = await self.get_artifact(artifact_id)

            await self.jr.mark_completed(job_id, worker_id)

            result = await self.rr.create_result(
                job_id=job_id,
                artifact_id=artifact_id,
                producer_id=worker_id,
                iteration=job.iteration,
                is_estimation=artifact.is_estimation(),
                is_model=artifact.is_model(),
                is_aggregation=True,
            )

            return result

        except NoResultFound:
            raise ValueError(f"job_id={job_id} not found")

    async def worker_error(self, job_id: str, worker_id: str) -> Result:
        try:
            LOGGER.warning(f"worker_id={worker_id}: creating aggregated result for job_id={job_id}")
            job = await self.jr.get_by_id(job_id)
            artifact_id = job.artifact_id

            # simple check
            await self.ar.get_artifact(artifact_id)

            artifact: Artifact = await self.get_artifact(artifact_id)

            await self.jr.mark_error(job_id, worker_id, job.iteration)

            result = await self.rr.create_result(
                job_id=job_id,
                artifact_id=artifact_id,
                producer_id=worker_id,
                iteration=job.iteration,
                is_estimation=artifact.is_estimation(),
                is_model=artifact.is_model(),
                is_aggregation=True,
                is_error=True,
            )

            return result

        except NoResultFound:
            raise ValueError(f"job_id={job_id} not found")

    async def aggregation_completed(self, job_id: str) -> None:
        LOGGER.info(f"job_id={job_id}: aggregation completed")

        job = await self.jr.get_by_id(job_id)
        artifact_id = job.artifact_id

        await self.ar.update_status(artifact_id, ArtifactJobStatus.COMPLETED)

        artifact: Artifact = await self.get_artifact(artifact_id)
        context = AggregationContext(
            artifact_id=artifact_id,
            current_iteration=job.iteration,
            next_iteration=job.iteration + 1,
        )

        await artifact.get_plan().post_aggregation_hook(context)

        if context.schedule_next_iteration:
            LOGGER.info(f"job_id={job_id}: scheduling next iteration ({context.next_iteration})")
            await self.submit_artifact(artifact, context.next_iteration)

    async def aggregation_error(self, job_id: str, error: str) -> None:
        LOGGER.warning(f"job_id={job_id}: aggregation completed with error: {error}")

        job = await self.jr.get_by_id(job_id)
        artifact_id = job.artifact_id

        await self.ar.update_status(artifact_id, ArtifactJobStatus.ERROR)

    async def save_metrics(self, metrics: Metrics):
        artifact = await self.ar.get_artifact(metrics.artifact_id)

        if artifact is None:
            raise ValueError(f"artifact_id={metrics.artifact_id} assigned to metrics not found")

        path = await self.ar.storage_location(artifact.artifact_id, f"metrics_{metrics.source}.json")

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(metrics.dict())
            await f.write(content)
