from abc import ABC, abstractmethod

from ferdelance.core.entity import Entity

from pydantic import SerializeAsAny


class Distribution(ABC, Entity):
    encryption: bool = True

    @abstractmethod
    def bind_locks(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
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


TDistribution = SerializeAsAny[Distribution]
