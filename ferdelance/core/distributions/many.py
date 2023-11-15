from ferdelance.core.distributions.core import Distribution


class Arrange(Distribution):
    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        locks: list[list[int]] = list()

        for _ in job_ids0:
            locks.append(job_ids1)

        return locks


class Distribute(Arrange):
    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        if len(job_ids0) != 1:
            raise ValueError("Cannot distribute from multiple sources, use Arrange instead")

        return super().bind(job_ids0, job_ids1)


class Collect(Arrange):
    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        if len(job_ids1) != 1:
            raise ValueError("Cannot collect to multiple sinks, use Arrange instead")

        return super().bind(job_ids0, job_ids1)
