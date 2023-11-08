from ferdelance.core.distributions.core import Distribution
from ferdelance.core.environment.core import Environment
from ferdelance.schemas.components import Component


class DirectToNext(Distribution):
    next: Component

    def distribute(self, env: Environment) -> None:
        return super().distribute(env)

    def bind(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        # TODO: this is a special case
        return []
