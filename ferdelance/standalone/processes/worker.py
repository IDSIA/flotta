from ferdelance.worker.tasks.aggregation import aggregate

from multiprocessing import Process, Queue
from multiprocessing.managers import BaseManager

import logging
import signal

LOGGER = logging.getLogger(__name__)


class LocalWorker(Process):
    def __init__(self):
        super().__init__()

        # BaseManager.register('get_queue') TODO: maybe we need this? Already defined in the main...
        self.m = BaseManager(address=("", 14560))
        self.m.connect()
        self.task_queue: Queue = self.m.get_queue()  # type: ignore
        self.stop = False

        # TODO: connect/join/register on server

    def run(self) -> None:
        def signal_handler(signum, frame):
            """This handler is used to gracefully stop when ctrl-c is hit in the terminal."""
            LOGGER.info("stopping worker")
            try:
                self.stop = True
                self.task_queue.put(None)
            except Exception:
                pass

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        while not self.stop:
            try:
                next_job = self.task_queue.get()
                if next_job is None:
                    self.task_queue.task_done()
                    break

                token, artifact_id, model_ids = next_job

                aggregate(token, artifact_id, model_ids)

                self.task_queue.task_done()
            except Exception as e:
                LOGGER.error(e)
