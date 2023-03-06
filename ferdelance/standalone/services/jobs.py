from ferdelance.database import AsyncSession
from ferdelance.server.services import JobManagementService
from ferdelance.standalone.extra import extra

from multiprocessing import Queue

import logging


LOGGER = logging.getLogger(__name__)


class JobManagementLocalService(JobManagementService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        if extra.AGGREGATION_QUEUE is None:
            raise ValueError("Could not run without a queue")

        self.aggregation_queue: Queue = extra.AGGREGATION_QUEUE

    def _start_aggregation(self, token: str, artifact_id: str, result_ids: list[str]) -> None:
        LOGGER.info("standalone: starting local aggregation")

        self.aggregation_queue.put((token, artifact_id, result_ids))
