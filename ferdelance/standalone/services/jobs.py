from ferdelance.database import AsyncSession
from ferdelance.server.services import JobManagementService

from multiprocessing.managers import BaseManager

import logging


LOGGER = logging.getLogger(__name__)


class JobManagementLocalService(JobManagementService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        # BaseManager.register('get_queue') TODO: maybe we need this? Already defined in the main...
        self.m = BaseManager(address=("", 14560))
        self.m.connect()

    def _start_aggregation(self, token: str, artifact_id: str, model_ids: list[str]) -> None:
        LOGGER.info("standalone: starting local aggregation")

        self.m.get_queue().put((token, artifact_id, model_ids))  # type: ignore
