from ferdelance.database.repositories import (
    Repository,
    AsyncSession,
    ArtifactRepository,
    DataSourceRepository,
    JobRepository,
    ResultRepository,
    ComponentRepository,
    ProjectRepository,
)
from ferdelance.schemas.artifacts import Artifact, ArtifactStatus
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.database import ServerArtifact, ServerTask
from ferdelance.schemas.components import Client
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.models import Metrics
from ferdelance.server.exceptions import ArtifactDoesNotExists, TaskDoesNotExists
from ferdelance.shared.status import JobStatus, ArtifactJobStatus
from ferdelance.worker.tasks import aggregation

from sqlalchemy.exc import NoResultFound

import aiofiles
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


class JobManagementService(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.cr: ComponentRepository = ComponentRepository(session)
        self.ar: ArtifactRepository = ArtifactRepository(session)
        self.dsr: DataSourceRepository = DataSourceRepository(session)
        self.jr: JobRepository = JobRepository(session)
        self.rr: ResultRepository = ResultRepository(session)
        self.pr: ProjectRepository = ProjectRepository(session)

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        try:
            # TODO: maybe split artifact for each client on submit?

            # TODO: manage for estimates

            artifact_db: ServerArtifact = await self.ar.create_artifact(artifact)

            project = await self.pr.get_by_id(artifact.project_id)
            datasources_ids = await self.pr.datasources_ids(project.token)

            for datasource_id in datasources_ids:
                client: Client = await self.dsr.get_client_by_datasource_id(datasource_id)

                await self.jr.schedule_job(artifact_db.artifact_id, client.client_id)

            return artifact_db.get_status()
        except ValueError as e:
            raise e

    async def get_artifact(self, artifact_id: str) -> Artifact:
        return await self.ar.load(artifact_id)

    async def client_local_model_start(self, artifact_id: str, client_id: str) -> ClientTask:
        try:
            artifact_db: ServerArtifact = await self.ar.get_artifact(artifact_id)

            if ArtifactJobStatus[artifact_db.status] == ArtifactJobStatus.SCHEDULED:
                await self.ar.update_status(artifact_id, ArtifactJobStatus.TRAINING)

            artifact_path = artifact_db.path

            if not os.path.exists(artifact_path):
                LOGGER.warning(
                    f"client_id={client_id}: artifact_id={artifact_id} does not exist with path={artifact_path}"
                )
                raise ArtifactDoesNotExists()

            async with aiofiles.open(artifact_path, "r") as f:
                data = await f.read()
                artifact = Artifact(**json.loads(data))

            hashes = await self.dsr.get_hash_by_client_and_project(client_id, artifact.project_id)

            if len(hashes) == 0:
                LOGGER.warning(f"client_id={client_id}: task has no datasources with artifact_id={artifact_id}")
                raise TaskDoesNotExists()

            # TODO: for complex training, filter based on artifact.load field

            job: Job = await self.jr.next_job_for_client(client_id)

            job: Job = await self.jr.start_execution(job)

            return ClientTask(artifact=artifact, datasource_hashes=hashes)

        except NoResultFound:
            LOGGER.warning(f"client_id={client_id}: task does not exists with artifact_id={artifact_id}")
            raise TaskDoesNotExists()

    def _start_aggregation(self, token: str, artifact_id: str, model_ids: list[str]) -> None:
        aggregation.delay(token, artifact_id, model_ids)

    async def client_local_model_completed(self, artifact_id: str, client_id: str) -> None:
        LOGGER.info(f"client_id={client_id}: started aggregation request")

        await self.jr.stop_execution(artifact_id, client_id)

        artifact: ServerArtifact = await self.ar.get_artifact(artifact_id)

        if artifact is None:
            LOGGER.error(f"Cannot aggregate: artifact_id={artifact_id} not found")
            return

        total = await self.jr.count_jobs_for_artifact(artifact_id)
        completed = await self.jr.count_jobs_by_status(artifact_id, JobStatus.COMPLETED)
        error = await self.jr.count_jobs_by_status(artifact_id, JobStatus.ERROR)

        if completed < total:
            LOGGER.info(f"Cannot aggregate: {completed} / {total} completed job(s)")
            return

        if error > 0:
            LOGGER.error(f"Cannot aggregate: {error} jobs have error")
            return

        LOGGER.info(f"All {total} job(s) completed, starting aggregation")

        token = await self.cr.get_token_by_client_type("WORKER")

        if token is None:
            LOGGER.error("Cannot aggregate: no worker available")
            return

        models: list[ServerTask] = await self.rr.get_models_by_artifact_id(artifact_id)

        model_ids: list[str] = [m.model_id for m in models]

        await self.ar.update_status(artifact.artifact_id, ArtifactJobStatus.AGGREGATING)

        self._start_aggregation(token, artifact_id, model_ids)

    async def aggregation_completed(self, artifact_id: str) -> None:
        LOGGER.info(f"aggregation completed for artifact_id={artifact_id}")
        await self.ar.update_status(artifact_id, ArtifactJobStatus.COMPLETED)

    async def save_metrics(self, metrics: Metrics):
        artifact = await self.ar.get_artifact(metrics.artifact_id)

        if artifact is None:
            raise ValueError(f"artifact_id={metrics.artifact_id} assigned to metrics not found")

        path = await self.ar.storage_location(artifact.artifact_id, f"metrics_{metrics.source}.json")

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(metrics.dict())
            await f.write(content)
