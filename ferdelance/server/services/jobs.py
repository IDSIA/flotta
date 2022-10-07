from ...database.services import DBSessionService, Session, ArtifactService, DataSourceService, JobService, ModelService, ClientService

from ...worker.tasks import aggregation

from ...config import STORAGE_ARTIFACTS

from ferdelance_shared.schemas import Artifact, ArtifactStatus
from ferdelance_shared.status import JobStatus, ArtifactJobStatus

from uuid import uuid4

import json
import logging
import os

LOGGER = logging.getLogger(__name__)


class JobManagementService(DBSessionService):
    def __init__(self, db: Session) -> None:
        super().__init__(db)

        self.cs: ClientService = ClientService(db)
        self.ars: ArtifactService = ArtifactService(db)
        self.dss: DataSourceService = DataSourceService(db)
        self.js: JobService = JobService(db)
        self.ms: ModelService = ModelService(db)

    def dump(self, artifact: Artifact) -> str:
        path = os.path.join(STORAGE_ARTIFACTS, f'{artifact.artifact_id}.json')

        with open(path, 'w') as f:
            json.dump(artifact.dict(), f)

        return path

    def load(self, artifact_id: str) -> Artifact:
        artifact = self.ars.get_artifact(artifact_id)

        if artifact is None:
            raise ValueError(f'artifact_id={artifact_id} not found')

        if not os.path.exists(artifact.path):
            raise ValueError(f'artifact_id={artifact_id} not found')

        with open(artifact.path, 'r') as f:
            return Artifact(**json.load(f))

    def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        artifact.artifact_id = str(uuid4())

        try:
            path = self.dump(artifact)

            artifact_db = self.ars.create_artifact(artifact.artifact_id, path, ArtifactJobStatus.SCHEDULED.name)

            client_ids = set()

            for query in artifact.dataset.queries:
                client_id: str = self.dss.get_client_id_by_datasource_id(query.datasources_id)

                if client_id in client_ids:
                    continue

                self.js.create_job(artifact.artifact_id, client_id, JobStatus.SCHEDULED)

                client_ids.add(client_id)

            return ArtifactStatus(
                artifact_id=artifact.artifact_id,
                status=artifact_db.status,
            )
        except ValueError as e:
            raise e

    def get_artifact(self, artifact_id: str) -> Artifact:
        return self.load(artifact_id)

    def aggregate(self, artifact_id: str, client_id) -> None:
        LOGGER.info(f'client_id={client_id}: started aggregation request')

        self.js.create_job(artifact_id, client_id, JobStatus.COMPLETED)

        artifact: Artifact = self.ars.get_artifact(artifact_id)

        total = self.js.count_jobs_by_status(artifact_id, JobStatus.SCHEDULED)
        completed = self.js.count_jobs_by_status(artifact_id, JobStatus.COMPLETED)
        error = self.js.count_jobs_by_status(artifact_id, JobStatus.ERROR)

        if completed < total:
            LOGGER.info(f'Cannot aggregate: {completed} / {total} completed job(s)')
            return

        if error > 0:
            LOGGER.error(f'Cannot aggregate: {error} jobs have error')
            return

        LOGGER.info(f'All {total} job(s) completed, starting aggregation')

        token: str = self.cs.get_token_by_client_type('WORKBENCH')

        model_ids: list[str] = [m.model_id for m in self.ms.get_models_by_artifact_id(artifact_id)]

        self.ars.update_status(artifact.artifact_id, ArtifactJobStatus.AGGREGATING)

        aggregation.delay(token, artifact_id, model_ids)

    def evaluate(self, artifact: Artifact) -> ArtifactStatus:
        # TODO
        raise NotImplementedError()
