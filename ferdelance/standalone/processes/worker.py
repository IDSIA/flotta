from ferdelance.worker.tasks.aggregation import aggregate

from multiprocessing import Process, Queue
from multiprocessing.managers import BaseManager

import signal


class LocalWorker(Process):
    def __init__(self):
        super().__init__()

        # BaseManager.register('get_queue') TODO: maybe we need this? Already defined in the main...
        self.m = BaseManager(address=("", 14560))
        self.m.connect()
        self.task_queue: Queue = self.m.get_queue()
        self.stop = False

        # TODO: connect/join/register on server

    def run(self):
        def main_signal_handler(signum, frame):
            """This handler is used to gracefully stop when ctrl-c is hit in the terminal."""
            self.stop = True
            self.task_queue.put(None)

        signal.signal(signal.SIGINT, main_signal_handler)
        signal.signal(signal.SIGTERM, main_signal_handler)

        while not self.stop:
            next_job = self.task_queue.get()
            if next_job is None:
                self.task_queue.task_done()
                break

            token, artifact_id, model_ids = next_job

            aggregate(token, artifact_id, model_ids)

            self.task_queue.task_done()

        return
