from ferdelance.worker.tasks.aggregation import AggregationTask

from multiprocessing import Process, Queue

import logging

LOGGER = logging.getLogger(__name__)


class LocalWorker(Process):
    def __init__(self, task_queue: Queue):
        super().__init__()
        self.task_queue: Queue = task_queue
        self.stop = False

        # TODO: connect/join/register on server

    def run(self) -> None:
        while not self.stop:
            try:
                next_job = self.task_queue.get()
                if next_job is None:
                    self.task_queue.task_done()
                    break

                token, job_id = next_job

                a = AggregationTask()
                a.setup(token)
                a.aggregate(job_id)

                self.task_queue.task_done()

            except KeyboardInterrupt:
                LOGGER.info("stopping worker")
                self.stop = True

            except Exception as e:
                LOGGER.error(e)
