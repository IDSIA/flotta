from ferdelance.worker.processes import LocalWorker

from multiprocessing import JoinableQueue


class ExtraConfig:
    def __init__(self) -> None:
        self.aggregation_queue: JoinableQueue | None = None
        self.estimation_queue: JoinableQueue | None = None
        self.training_queue: JoinableQueue | None = None

        self.aggregation_workers: list[LocalWorker] = []
        self.estimation_workers: list[LocalWorker] = []
        self.training_workers: list[LocalWorker] = []


extra: ExtraConfig = ExtraConfig()
