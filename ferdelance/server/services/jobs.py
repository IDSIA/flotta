from ...database.services import DBSessionService, Session, ArtifactService, DataSourceService, JobService

from ...worker.tasks import aggregation

from ..config import STORAGE_ARTIFACTS

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

        self.ars: ArtifactService = ArtifactService(db)
        self.dss: DataSourceService = DataSourceService(db)
        self.js: JobService = JobService(db)

    def write_to_disk(self, artifact: Artifact) -> str:
        path = os.path.join(STORAGE_ARTIFACTS, f'{artifact.artifact_id}.json')

        with open(path, 'w') as f:
            json.dump(artifact.dict(), f)

        return path

    def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        artifact_id = str(uuid4())
        artifact.artifact_id = artifact_id

        try:
            path = self.write_to_disk(artifact)

            artifact_db = self.ars.create_artifact(artifact_id, path, ArtifactJobStatus.SCHEDULED.name)

            client_ids = set()

            for query in artifact.dataset.queries:
                client_id: str = self.dss.get_client_id_by_datasource_id(query.datasources_id)

                if client_id in client_ids:
                    continue

                self.js.create_job(artifact.artifact_id, client_id, JobStatus.SCHEDULED)

                client_ids.add(client_id)

            return ArtifactStatus(
                artifact_id=artifact_id,
                status=artifact_db.status,
            )
        except ValueError as e:
            raise e

    def aggregate(self, artifact: Artifact) -> ArtifactStatus:

        jobs = self.js.get_tasks_for_artifact(artifact.artifact_id)

        total = len(jobs)
        completed = len(j for j in jobs if JobStatus[j.status] == JobStatus.COMPLETED)

        if completed < total:
            LOGGER.info(f'Cannot aggregate: completed={completed} / {total} job(s)')
            return

        LOGGER.info(f'All {total} job(s) completed, starting aggregation')

        new_status = ArtifactJobStatus.AGGREGATING.name
        self.ars.update_status(artifact.artifact_id, new_status)

        aggregation.delay(artifact.dict())

        return ArtifactStatus(
            artifact_id=artifact.artifact_id,
            status=new_status,
        )

    def evaluate(self, artifact: Artifact) -> ArtifactStatus:
        # TODO
        raise NotImplementedError()
