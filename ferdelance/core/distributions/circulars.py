from ferdelance.core.distributions.core import Distribution
from ferdelance.core.environment.core import Environment


class RoundRobin(Distribution):
    def distribute(self, env: Environment) -> None:
        return super().distribute(env)

    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        # TODO
        return super().bind(job_ids0, job_ids1)
