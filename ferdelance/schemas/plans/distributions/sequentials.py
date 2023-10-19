from ferdelance.schemas.plans.distributions.core import Distribution


class Sequential(Distribution):
    def distribute(self) -> None:
        return super().distribute()
