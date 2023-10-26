from abc import ABC, abstractmethod

from ferdelance.core.entity import Entity
from ferdelance.core.environment.core import Environment


class Distribution(ABC, Entity):
    @abstractmethod
    def distribute(self, env: Environment) -> None:
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
    def distribute(self, env: Environment) -> None:
        return super().distribute(env)

    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        locks: list[list[int]] = list()

        for _ in job_ids0:
            locks.append(job_ids1)

        return locks


class Distribute(Arrange):
    def distribute(self, env: Environment) -> None:
        return super().distribute(env)

    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        if len(job_ids0) != 1:
            raise ValueError("Cannot distribute from multiple sources, use Arrange instead")

        return super().bind(job_ids0, job_ids1)


class Collect(Arrange):
    def distribute(self, env: Environment) -> None:
        return super().distribute(env)

    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        if len(job_ids1) != 1:
            raise ValueError("Cannot collect to multiple sinks, use Arrange instead")

        return super().bind(job_ids0, job_ids1)
