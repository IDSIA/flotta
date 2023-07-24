from multiprocessing import JoinableQueue


class ExtraConfig:
    def __init__(self) -> None:
        self.aggregation_queue: JoinableQueue | None = None
        self.estimation_queue: JoinableQueue | None = None
        self.training_queue: JoinableQueue | None = None

        self.aggregation_workers: list = list()
        self.estimation_workers: list = list()
        self.training_workers: list = list()


extra: ExtraConfig = ExtraConfig()
