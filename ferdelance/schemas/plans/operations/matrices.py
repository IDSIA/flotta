from typing import Any

from ferdelance.schemas.plans.operations.core import Operation

import numpy as np


class UniformMatrix(Operation):
    def __init__(
        self,
        size: tuple[int, ...],
        low: float = 0.0,
        high: float = 1.0,
        env_names: list[str] = list(),
        persist: bool = False,  # <- save the created environment on disk for reuse!
        random_seed: Any = None,
    ) -> None:
        super().__init__(UniformMatrix.__name__, list(), env_names, random_seed)

        self.size: tuple[int, ...] = size
        self.low: float = low
        self.high: float = high

        self.persist: bool = persist

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "size": self.size,
            "low": self.low,
            "high": self.high,
            "persist": self.persist,
        }

    def exec(self, env: dict[str, Any]) -> dict[str, Any]:
        r = np.random.default_rng(self.random_seed)

        for e_out in self.env_names:
            env[e_out] = r.uniform(size=self.size)

        return env


class SumMatrix(Operation):
    def __init__(
        self,
        data_inputs: list[str] = list(),
        env_inputs: list[str] = list(),
        random_seed: Any = None,
    ) -> None:
        super().__init__(SumMatrix.__name__, data_inputs, env_inputs, random_seed)

    def exec(self, env: dict[str, Any]) -> dict[str, Any]:
        for d_in, e_out in zip(self.data_names, self.env_names):
            env[e_out] = env[e_out] + env[d_in]

        return env


class SubtractMatrix(Operation):
    def __init__(
        self,
        data_inputs: list[str] = list(),
        env_inputs: list[str] = list(),
        use_persisted: bool = False,
        random_seed: Any = None,
    ) -> None:
        super().__init__(SubtractMatrix.__name__, data_inputs, env_inputs, random_seed)

        self.use_persisted: bool = use_persisted

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "use_persisted": self.use_persisted,
        }

    def exec(self, env: dict[str, Any]) -> dict[str, Any]:
        for d_in, e_out in zip(self.data_names, self.env_names):
            env[e_out] = env[e_out] - env[d_in]

        return env
