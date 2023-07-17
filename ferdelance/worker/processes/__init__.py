from ferdelance.client.services.actions.execute import ExecuteAction, ExecutionResult
from ferdelance.client.config import Config
from ferdelance.client.services.routes import RouteService
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.updates import UpdateExecute

from multiprocessing import Process, JoinableQueue

import logging

LOGGER = logging.getLogger(__name__)


class ClientWorker(Process):
    def __init__(self, config: Config, idx: str, queue: JoinableQueue) -> None:
        super().__init__()

        LOGGER.info(f"creating client-worker_idx={idx}")

        self.routes_service: RouteService = RouteService(config)
        self.action = ExecuteAction(config.data)

        self.queue: JoinableQueue = queue
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

                update_execute: UpdateExecute = UpdateExecute(**data)
                task: ClientTask = self.routes_service.get_task(update_execute)

                res: ExecutionResult = self.action.execute(task)

                if res.is_estimate:
                    self.routes_service.post_result(res.job_id, res.path)

                if res.is_model:
                    for m in res.metrics:
                        m.job_id = res.job_id
                        self.routes_service.post_metrics(m)

                    self.routes_service.post_result(res.job_id, res.path)

        except KeyboardInterrupt:
            LOGGER.info(f"{self.idx}: Caught KeyboardInterrupt! Stopping operations")
            self.stop = True
