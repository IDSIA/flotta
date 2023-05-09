from ferdelance.database.repositories import (
    Repository,
    AsyncSession,
    AggregationContext,
)
from ferdelance.schemas.artifacts import Artifact, ArtifactStatus
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.database import ServerArtifact, Result
from ferdelance.schemas.components import Client
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.models import Metrics
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

        self.context: AggregationContext = AggregationContext(session)

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        try:
            # TODO: maybe split artifact for each client on submit?

            # TODO: manage for estimates

            artifact_db: ServerArtifact = await self.context.ar.create_artifact(artifact)

            project = await self.context.pr.get_by_id(artifact.project_id)
            datasources_ids = await self.context.pr.list_datasources_ids(project.token)

            for datasource_id in datasources_ids:
                client: Client = await self.context.dsr.get_client_by_datasource_id(datasource_id)

                await self.context.jr.schedule_job(
                    artifact_db.artifact_id,
                    client.client_id,
                    is_model=artifact.is_model(),
                    is_estimation=artifact.is_estimation(),
                )

            return artifact_db.get_status()
        except ValueError as e:
            raise e

    async def get_artifact(self, artifact_id: str) -> Artifact:
        return await self.context.ar.load(artifact_id)

    async def client_task_start(self, artifact_id: str, client_id: str) -> ClientTask:
        try:
            artifact_db: ServerArtifact = await self.context.ar.get_artifact(artifact_id)

            if ArtifactJobStatus[artifact_db.status] == ArtifactJobStatus.SCHEDULED:
                await self.context.ar.update_status(artifact_id, ArtifactJobStatus.TRAINING)

            artifact_path = artifact_db.path

            if not os.path.exists(artifact_path):
                LOGGER.warning(
                    f"client_id={client_id}: artifact_id={artifact_id} does not exist with path={artifact_path}"
                )
                raise ArtifactDoesNotExists()

            async with aiofiles.open(artifact_path, "r") as f:
                data = await f.read()
                artifact = Artifact(**json.loads(data))

            hashes = await self.context.dsr.list_hash_by_client_and_project(client_id, artifact.project_id)

            if len(hashes) == 0:
                LOGGER.warning(f"client_id={client_id}: task has no datasources with artifact_id={artifact_id}")
                raise TaskDoesNotExists()

            # TODO: for complex training, filter based on artifact.load field

            job: Job = await self.context.jr.next_job_for_component(client_id)

            await self.context.jr.start_execution(job)

            return ClientTask(artifact=artifact, datasource_hashes=hashes)

        except NoResultFound:
            LOGGER.warning(f"client_id={client_id}: task does not exists with artifact_id={artifact_id}")
            raise TaskDoesNotExists()

    async def worker_task_start(self, artifact_id: str, client_id: str) -> None:
        try:
            artifact_db: ServerArtifact = await self.context.ar.get_artifact(artifact_id)

            if ArtifactJobStatus[artifact_db.status] != ArtifactJobStatus.AGGREGATING:
                raise ValueError("Wrong status for artifact")

            job: Job = await self.context.jr.next_job_for_component(client_id)

            await self.context.jr.start_execution(job)

        except NoResultFound:
            LOGGER.warning(f"client_id={client_id}: task does not exists with artifact_id={artifact_id}")
            raise TaskDoesNotExists()

    def _start_aggregation(self, token: str, artifact_id: str, result_ids: list[str]) -> str:
        LOGGER.info(f"artifact_id={artifact_id}: started aggregation task with ({len(result_ids)}) result(s)")
        task: AsyncResult = aggregation.apply_async(
            args=[
                token,
                artifact_id,
                result_ids,
            ],
        )
        task_id = str(task.task_id)
        LOGGER.info(f"artifact_id={artifact_id}: scheduled task with celery_id={task_id} status={task.status}")
        return task_id

    async def client_result_create(self, artifact_id: str, client_id: str) -> Result:
        LOGGER.info(f"client_id={client_id}: creating results")

        # simple check
        await self.context.ar.get_artifact(artifact_id)

        artifact: Artifact = await self.context.ar.load(artifact_id)

        await self.context.jr.mark_completed(artifact_id, client_id)

        result = await self.context.rr.create_result(
            artifact_id,
            client_id,
            artifact.is_estimation(),
            artifact.is_model(),
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
            total = await self.context.jr.count_jobs_by_artifact_id(artifact_id)
            completed = await self.context.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.COMPLETED)
            error = await self.context.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.ERROR)

            if completed < total:
                LOGGER.info(
                    f"artifact_id={result.artifact_id}: cannot aggregate: {completed} / {total} completed job(s)"
                )
                return

            if error > 0:
                LOGGER.error(f"artifact_id={result.artifact_id}: cannot aggregate: {error} jobs have error")
                return

            LOGGER.info(f"artifact_id={result.artifact_id}: all {total} job(s) completed, starting aggregation")

            token = await self.context.cr.get_token_for_workers()

            if token is None:
                LOGGER.error(f"artifact_id={result.artifact_id}: cannot aggregate: no worker available")
                return

            # schedule an aggregation
            worker_id = await self.context.cr.get_component_id_by_token(token)

            job: Job = await self.context.jr.schedule_job(
                artifact_id,
                worker_id,
                is_model=result.is_model,
                is_estimation=result.is_estimation,
                is_aggregation=True,
            )

            results: list[Result] = await self.context.rr.list_results_by_artifact_id(artifact_id)
            result_ids: list[str] = [m.result_id for m in results]

            artifact: Artifact = await self.context.ar.load(artifact_id)

            await artifact.get_plan().pre_aggregation_hook(artifact_id, self.context)

            task_id: str = self._start_aggregation(token, artifact_id, result_ids)

            await self.context.ar.update_status(artifact_id, ArtifactJobStatus.AGGREGATING)
            await self.context.jr.set_celery_id(job, str(task_id))

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

    async def worker_result_create(self, artifact_id: str, worker_id: str) -> Result:
        try:
            LOGGER.info(f"worker_id={worker_id}: creating aggregated result for artifact_id={artifact_id}")
            # simple check
            await self.context.ar.get_artifact(artifact_id)

            artifact: Artifact = await self.context.ar.load(artifact_id)

            await self.context.jr.mark_completed(artifact_id, worker_id)

            result = await self.context.rr.create_result(
                artifact_id=artifact_id,
                producer_id=worker_id,
                is_estimation=artifact.is_estimation(),
                is_model=artifact.is_model(),
                is_aggregation=True,
            )

            return result

        except NoResultFound:
            raise ValueError(f"artifact_id={artifact_id} not found")

    async def worker_error(self, artifact_id: str, worker_id: str) -> Result:
        try:
            LOGGER.warning(f"worker_id={worker_id}: creating aggregated result for artifact_id={artifact_id}")
            # simple check
            await self.context.ar.get_artifact(artifact_id)

            artifact: Artifact = await self.context.ar.load(artifact_id)

            await self.context.jr.mark_error(artifact_id, worker_id)

            result = await self.context.rr.create_result(
                artifact_id,
                worker_id,
                artifact.is_estimation(),
                artifact.is_model(),
                True,
                True,
            )

            return result

        except NoResultFound:
            raise ValueError(f"artifact_id={artifact_id} not found")

    async def aggregation_completed(self, artifact_id: str) -> None:
        LOGGER.info(f"artifact_id={artifact_id}: aggregation completed")
        await self.context.ar.update_status(artifact_id, ArtifactJobStatus.COMPLETED)

        artifact: Artifact = await self.context.ar.load(artifact_id)

        await artifact.get_plan().post_aggregation_hook(artifact_id, self.context)

    async def aggregation_error(self, artifact_id: str, error: str) -> None:
        LOGGER.warning(f"artifact_id={artifact_id}: aggregation completed with error: {error}")
        await self.context.ar.update_status(artifact_id, ArtifactJobStatus.ERROR)

    async def save_metrics(self, metrics: Metrics):
        artifact = await self.context.ar.get_artifact(metrics.artifact_id)

        if artifact is None:
            raise ValueError(f"artifact_id={metrics.artifact_id} assigned to metrics not found")

        path = await self.context.ar.storage_location(artifact.artifact_id, f"metrics_{metrics.source}.json")

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(metrics.dict())
            await f.write(content)
