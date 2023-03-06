from multiprocessing import Queue


class ExtraConfig:
    def __init__(self) -> None:
        self.AGGREGATION_QUEUE: Queue | None = None


extra: ExtraConfig = ExtraConfig()
