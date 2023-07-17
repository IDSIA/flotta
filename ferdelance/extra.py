from ferdelance.worker.processes import ClientWorker

from multiprocessing import JoinableQueue


class ExtraConfig:
    def __init__(self) -> None:
        self.aggregation_queue: JoinableQueue | None = None
        self.estimation_queue: JoinableQueue | None = None
        self.training_queue: JoinableQueue | None = None

        self.aggregation_workers: list[ClientWorker] = []
        self.estimation_workers: list[ClientWorker] = []
        self.training_workers: list[ClientWorker] = []


extra: ExtraConfig = ExtraConfig()
