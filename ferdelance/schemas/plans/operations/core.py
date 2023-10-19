from abc import abstractmethod
from typing import Any
from pydantic import BaseModel


class Op(BaseModel):
    name: str
    params: dict[str, Any]


class Operation:
    def __init__(
        self,
        name: str,
        data_names: list[str] = list(),
        env_names: list[str] = list(),
        random_seed: Any = None,
    ) -> None:
        super().__init__()

        self.name: str = name
        self.random_seed: Any = random_seed

        self.data_names: list[str] = data_names  # variables from local data
        self.env_names: list[str] = env_names  # variables in the environment

    def params(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "random_seed": self.random_seed,
            "data_names": self.data_names,
            "env_names": self.env_names,
        }

    def build(self) -> Op:
        return Op(
            name=self.name,
            params=self.params(),
        )

    @abstractmethod
    def exec(self, env: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError()
