from ferdelance.worker.jobs import JobService
from ferdelance.jobs_backend import TaskArguments

from multiprocessing import Process, JoinableQueue

import logging

LOGGER = logging.getLogger(__name__)


class LocalWorker(Process):
    def __init__(self, idx: str, queue: JoinableQueue) -> None:
        super().__init__()

        LOGGER.info(f"creating client-worker_idx={idx}")

        self.queue: JoinableQueue = queue
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

                js: JobService = JobService(args)
                js.run()

        except KeyboardInterrupt:
            LOGGER.info(f"{self.idx}: Caught KeyboardInterrupt! Stopping operations")
            self.stop = True
