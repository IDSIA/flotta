from ferdelance.database import AsyncSession
from ferdelance.jobs.server import JobManagementService
from ferdelance.standalone.extra import extra

from multiprocessing import Queue
from uuid import uuid4

import logging


LOGGER = logging.getLogger(__name__)


class JobManagementLocalService(JobManagementService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        if extra.aggregation_queue is None:
            raise ValueError("Could not run without a queue")

        self.aggregation_queue: Queue = extra.aggregation_queue

    def _start_aggregation(self, token: str, job_id: str, artifact_id: str) -> str:
        LOGGER.info(f"standalone: starting local aggregation for artifact_id={artifact_id}")

        self.aggregation_queue.put((token, job_id))

        return f"local-{str(uuid4())}"
