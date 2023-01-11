from ferdelance.shared.artifacts import (
    Artifact,
    ArtifactStatus,
)
from ferdelance.server.services import JobManagementService
from ferdelance.config import conf
from ferdelance.database import AsyncSession

from multiprocessing import Pool

import logging

LOGGER = logging.getLogger(__name__)


pool = Pool(conf.STANDALONE_WORKERS)

LOGGER.info('yay')


class JobManagementLocalService(JobManagementService):

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        # TODO
        LOGGER.info('standalone: submit artifact')
        raise NotImplementedError()

    async def client_local_model_start(self, artifact_id: str, client_id: str) -> Artifact:
        # TODO
        LOGGER.info('standalone: client local model start')
        raise NotImplementedError()

    async def client_local_model_completed(self, artifact_id: str, client_id: str) -> None:
        # TODO
        LOGGER.info('standalone: client local model completed')
        raise NotImplementedError()
