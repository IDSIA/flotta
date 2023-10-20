from abc import abstractmethod
from typing import Any

from ferdelance.schemas.plans.entity import Entity


class Operation(Entity):
    data_names: list[str] = list()  # variables from local data
    env_names: list[str] = list()  # variables in the environment
    random_seed: Any = (None,)

    @abstractmethod
    def exec(self, env: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError()
