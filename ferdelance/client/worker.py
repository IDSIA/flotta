from ferdelance.client.services.actions.execute import ExecuteAction
from ferdelance.client.config import Config
from ferdelance.schemas.updates import UpdateExecute

from multiprocessing import Process, Queue

import logging

LOGGER = logging.getLogger(__name__)


class ClientWorker(Process):
    def __init__(self, config: Config, idx: str, queue: Queue) -> None:
        super().__init__()

        LOGGER.info(f"creating client-worker_idx={idx}")

        self.config: Config = config
        self.queue: Queue = queue
        self.idx: str = idx
        self.stop: bool = False

    def run(self) -> None:
        try:
            while not self.stop:
                data: dict | None = self.queue.get()

                if data is None:
                    self.stop = True
                    LOGGER.info(f"stopping worker {self.idx}")
                    continue

                action = ExecuteAction(self.config, UpdateExecute(**data))
                action.execute()

        except KeyboardInterrupt:
            LOGGER.info(f"{self.idx}: Caught KeyboardInterrupt! Stopping operations")
            self.stop = True
