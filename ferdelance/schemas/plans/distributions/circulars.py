from ferdelance.schemas.plans.distributions.core import Distribution


class RoundRobin(Distribution):
    def distribute(self) -> None:
        return super().distribute()
