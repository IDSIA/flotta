from abc import ABC, abstractclassmethod
from ferdelance.schemas.worker import TaskArguments


class Backend(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractclassmethod
    def start_aggregation(self, args: TaskArguments) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def start_training(self, args: TaskArguments) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def start_estimation(self, args: TaskArguments) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def stop_backend(self):
        raise NotImplementedError()
