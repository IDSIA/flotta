from typing import Any

from abc import ABC, abstractmethod

from pydantic import BaseModel


class Distr(BaseModel):
    name: str
    params: dict[str, Any]


class Distribution(ABC):
    def __init__(self, name: str) -> None:
        super().__init__()

        self.name = name

    def params(self) -> dict[str, Any]:
        return {}

    def build(self) -> Distr:
        return Distr(
            name=self.name,
            params=self.params(),
        )

    @abstractmethod
    def distribute(self, env: dict[str, Any]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        """Defines the locks between the given job ids lists. The locks are
        applied to jobs0. When one of these jobs will be completed, it will
        unlock the jobs in the jobs1 list.

        The results of jobs0 will be distributed to jobs1, following the locks.

        Args:
            job_ids0 (list[int]):
                List of jobs that will be locking.
            job_ids1 (list[int]):
                List of jobs that will be unlocked.

        Raises:
            NotImplementedError:
                If this method is not available

        Returns:
            list[list[int]]:
                The list of locks (ids of jobs1) to apply to each job in jobs0.
        """
        raise NotImplementedError()


class Arrange(Distribution):
    def __init__(self) -> None:
        super().__init__(Arrange.__name__)

    def distribute(self, env: dict[str, Any]) -> None:
        return super().distribute(env)

    def bind(self, jobs0: list[int], jobs1: list[int]) -> list[list[int]]:
        locks: list[list[int]] = list()

        for _ in jobs0:
            locks.append(jobs1)

        return locks


class Distribute(Arrange):
    def __init__(self) -> None:
        super().__init__()
        self.name = Distribute.__name__

    def distribute(self, env: dict[str, Any]) -> None:
        return super().distribute(env)

    def bind(self, jobs0: list[int], jobs1: list[int]) -> list[list[int]]:
        if len(jobs0) != 1:
            raise ValueError("Cannot distribute from multiple sources, use Arrange!")

        return super().bind(jobs0, jobs1)


class Collect(Arrange):
    def __init__(self) -> None:
        super().__init__()
        self.name = Collect.__name__

    def distribute(self, env: dict[str, Any]) -> None:
        return super().distribute(env)

    def bind(self, jobs0: list[int], jobs1: list[int]) -> list[list[int]]:
        if len(jobs1) != 1:
            raise ValueError("Cannot collect to multiple sinks, use Arrange!")

        return super().bind(jobs0, jobs1)
