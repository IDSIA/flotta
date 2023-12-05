from ferdelance.core.distributions.core import Distribution


class RoundRobin(Distribution):
    def bind_locks(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        locks = []

        if len(job_ids0) != len(job_ids1):
            raise ValueError("Different amount of jobs between previous and next step")

        n = len(job_ids0)

        for i in range(n):
            x = (i + 1) % n

            locks += [job_ids1[x]]

        return locks
