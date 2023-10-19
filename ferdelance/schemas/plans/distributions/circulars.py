from typing import Any
from ferdelance.schemas.plans.distributions.core import Distribution


class RoundRobin(Distribution):
    def distribute(self, env: dict[str, Any]) -> None:
        return super().distribute(env)

    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        return super().bind(job_ids0, job_ids1)
