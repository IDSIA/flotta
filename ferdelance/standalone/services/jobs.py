from ferdelance.config import conf
from ferdelance.database import AsyncSession
from ferdelance.database.schemas import Client
from ferdelance.server.services import JobManagementService
from ferdelance.shared.artifacts import (
    Artifact,
    ArtifactStatus,
)
from ferdelance.shared.status import ArtifactJobStatus

from uuid import uuid4

import logging


LOGGER = logging.getLogger(__name__)


class JobManagementLocalService(JobManagementService):

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

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

    async def client_local_model_start(self, artifact_id: str, client_id: str) -> Artifact:
        # TODO
        LOGGER.info('standalone: client local model start')
        raise NotImplementedError()

    async def client_local_model_completed(self, artifact_id: str, client_id: str) -> None:
        # TODO
        LOGGER.info('standalone: client local model completed')
        raise NotImplementedError()
