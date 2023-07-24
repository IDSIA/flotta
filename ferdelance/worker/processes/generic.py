from ferdelance.schemas.worker import TaskArguments
from ferdelance.worker.jobs.services import JobService

from multiprocessing import Process, JoinableQueue

import logging

LOGGER = logging.getLogger(__name__)


class GenericProcess(Process):
    def __init__(self, idx: str, queue: JoinableQueue, job_service: JobService) -> None:
        super().__init__()

        LOGGER.info(f"creating client-worker_idx={idx}")

        self.queue: JoinableQueue = queue
        self.job_service: JobService = job_service
        self.idx: str = idx
        self.stop: bool = False

    def run(self) -> None:
        try:
            while not self.stop:
                raw_args: dict | None = self.queue.get()

                if raw_args is None:
                    self.stop = True
                    LOGGER.info(f"stopping worker {self.idx}")
                    continue

                args: TaskArguments = TaskArguments(**raw_args)

                LOGGER.info(f"running job_id={args.job_id}")

                self.job_service.run()

        except KeyboardInterrupt:
            LOGGER.info(f"{self.idx}: Caught KeyboardInterrupt! Stopping operations")
            self.stop = True
