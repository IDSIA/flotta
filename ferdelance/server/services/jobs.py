from ferdelance.config import conf
from ferdelance.database.schemas import Client
from ferdelance.database.services import (
    DBSessionService,
    AsyncSession,
    ArtifactService,
    DataSourceService,
    JobService,
    ModelService,
    ClientService
)

from ferdelance.server.exceptions import ArtifactDoesNotExists, TaskDoesNotExists

from ferdelance.shared.artifacts import Artifact, ArtifactStatus
from ferdelance.shared.models import Metrics
from ferdelance.shared.status import JobStatus, ArtifactJobStatus

from ferdelance.worker.tasks import aggregation

from uuid import uuid4

import aiofiles
import aiofiles.os as aos
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


class JobManagementService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.cs: ClientService = ClientService(session)
        self.ars: ArtifactService = ArtifactService(session)
        self.dss: DataSourceService = DataSourceService(session)
        self.js: JobService = JobService(session)
        self.ms: ModelService = ModelService(session)

    async def storage_dir(self, artifact_id) -> str:
        out_dir = os.path.join(conf.STORAGE_ARTIFACTS, artifact_id)
        await aos.makedirs(out_dir, exist_ok=True)
        return out_dir

    async def dump_artifact(self, artifact: Artifact) -> str:
        out_dir = await self.storage_dir(artifact.artifact_id)

        path = os.path.join(out_dir, 'descriptor.json')

        with open(path, 'w') as f:
            json.dump(artifact.dict(), f)

        return path

    async def load_artifact(self, artifact_id: str) -> Artifact:
        artifact = await self.ars.get_artifact(artifact_id)

        if artifact is None:
            raise ValueError(f'artifact_id={artifact_id} not found')

        if not os.path.exists(artifact.path):
            raise ValueError(f'artifact_id={artifact_id} not found')

        with open(artifact.path, 'r') as f:
            return Artifact(**json.load(f))

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        artifact.artifact_id = str(uuid4())

        try:
            path = await self.dump_artifact(artifact)

            artifact_db = await self.ars.create_artifact(artifact.artifact_id, path, ArtifactJobStatus.SCHEDULED.name)

            client_ids = set()

            for query in artifact.dataset.queries:
                client: Client = await self.dss.get_client_by_datasource_id(query.datasource_id)

                if client.client_id in client_ids:
                    continue

                await self.js.schedule_job(artifact.artifact_id, client.client_id)

                client_ids.add(client.client_id)

            return ArtifactStatus(
                artifact_id=artifact.artifact_id,
                status=artifact_db.status,
            )
        except ValueError as e:
            raise e

    async def get_artifact(self, artifact_id: str) -> Artifact:
        return await self.load_artifact(artifact_id)

    async def client_local_model_start(self, artifact_id: str, client_id: str) -> Artifact:
        artifact_db = await self.ars.get_artifact(artifact_id)

        if artifact_db is None:
            LOGGER.warning(f'client_id={client_id}: artifact_id={artifact_id} does not exists')
            raise ArtifactDoesNotExists()

        if ArtifactJobStatus[artifact_db.status] == ArtifactJobStatus.SCHEDULED:
            await self.ars.update_status(artifact_id, ArtifactJobStatus.TRAINING)

        artifact_path = artifact_db.path

        if not os.path.exists(artifact_path):
            LOGGER.warning(f'client_id={client_id}: artifact_id={artifact_id} does not exist with path={artifact_path}')
            raise ArtifactDoesNotExists()

        async with aiofiles.open(artifact_path, 'r') as f:
            data = await f.read()
            artifact = Artifact(**json.loads(data))

        client_datasource_ids = await self.dss.get_datasource_ids_by_client_id(client_id)

        artifact.dataset.queries = [q for q in artifact.dataset.queries if q.datasource_id in client_datasource_ids]

        job = await self.js.next_job_for_client(client_id)

        if job is None:
            LOGGER.warning(f'client_id={client_id}: task does not exists with artifact_id={artifact_id}')
            raise TaskDoesNotExists()

        job = await self.js.start_execution(job)

        return Artifact(
            artifact_id=job.artifact_id,
            dataset=artifact.dataset,
            model=artifact.model,
        )

    async def client_local_model_completed(self, artifact_id: str, client_id: str) -> None:
        LOGGER.info(f'client_id={client_id}: started aggregation request')

        await self.js.stop_execution(artifact_id, client_id)

        artifact = await self.ars.get_artifact(artifact_id)

        if artifact is None:
            LOGGER.error(f'Cannot aggregate: artifact_id={artifact_id} not found')
            return

        total = await self.js.count_jobs_for_artifact(artifact_id)
        completed = await self.js.count_jobs_by_status(artifact_id, JobStatus.COMPLETED)
        error = await self.js.count_jobs_by_status(artifact_id, JobStatus.ERROR)

        if completed < total:
            LOGGER.info(f'Cannot aggregate: {completed} / {total} completed job(s)')
            return

        if error > 0:
            LOGGER.error(f'Cannot aggregate: {error} jobs have error')
            return

        LOGGER.info(f'All {total} job(s) completed, starting aggregation')

        token = await self.cs.get_token_by_client_type('WORKER')
        models = await self.ms.get_models_by_artifact_id(artifact_id)

        model_ids: list[str] = [m.model_id for m in models]

        await self.ars.update_status(artifact.artifact_id, ArtifactJobStatus.AGGREGATING)

        aggregation.delay(token, artifact_id, model_ids)

    async def aggregation_completed(self, artifact_id: str) -> None:
        LOGGER.info(f'aggregation completed for artifact_id={artifact_id}')
        await self.ars.update_status(artifact_id, ArtifactJobStatus.COMPLETED)

    def evaluate(self, artifact: Artifact) -> ArtifactStatus:
        # TODO
        raise NotImplementedError()

    async def save_metrics(self, metrics: Metrics):
        artifact = await self.ars.get_artifact(metrics.artifact_id)

        if artifact is None:
            raise ValueError(f'artifact_id={metrics.artifact_id} assigned to metrics not found')

        out_dir = await self.storage_dir(artifact.artifact_id)

        path = os.path.join(out_dir, f'{artifact.artifact_id}_metrics_{metrics.source}.json')

        with open(path, 'w') as f:
            json.dump(metrics.dict(), f)
