from ferdelance.schemas.worker import TaskArguments
from ferdelance.worker.tasks.aggregation import AggregationTask

from multiprocessing import Process, JoinableQueue

import logging

LOGGER = logging.getLogger(__name__)


class LocalWorker(Process):
    def __init__(self, task_queue: JoinableQueue):
        super().__init__()
        self.task_queue: JoinableQueue = task_queue
        self.stop = False

        # TODO: connect/join/register on server

    def run(self) -> None:
        while not self.stop:
            try:
                raw_args = self.task_queue.get()
                if raw_args is None:
                    self.task_queue.task_done()
                    break

                args: TaskArguments = TaskArguments(**raw_args)

                a = AggregationTask()
                a.setup(args)
                a.aggregate()

                self.task_queue.task_done()

            except KeyboardInterrupt:
                LOGGER.info("stopping worker")
                self.stop = True

            except Exception as e:
                LOGGER.error(e)
