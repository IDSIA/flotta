from typing import Any

from ferdelance.schemas.plans.operations.core import Operation


class Define(Operation):
    def __init__(
        self,
        env_names: list[str] = ["model"],
        random_seed: Any = None,
    ) -> None:
        super().__init__(Define.__name__, list(), env_names, random_seed)

        # TODO: check how fdl.schemas.models.core work and adapt them to work there
