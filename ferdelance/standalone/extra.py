from multiprocessing import Queue


class ExtraConfig:
    def __init__(self) -> None:
        self.aggregation_queue: Queue | None = None


extra: ExtraConfig = ExtraConfig()
