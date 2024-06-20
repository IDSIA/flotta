from flotta.core.environment import Environment
from flotta.core.operations.core import Operation

from pydantic import field_validator

import numpy as np


class UniformMatrix(Operation):
    size: tuple[int, ...]
    low: float = 0.0
    high: float = 1.0
    persist: bool = False  # <- save the created environment on disk for reuse!

    @field_validator("data_names")
    def set_data_names(cls, _) -> list[str]:
        return list()

    def exec(self, env: Environment) -> Environment:
        r = np.random.default_rng(self.random_state)

        for e_out in self.env_names:
            env[e_out] = r.uniform(size=self.size)

        return env


class SumMatrix(Operation):
    def exec(self, env: Environment) -> Environment:
        for d_in, e_out in zip(self.data_names, self.env_names):
            env[e_out] = env[e_out] + env[d_in]

        return env


class SubtractMatrix(Operation):
    use_persisted: bool = False

    def exec(self, env: Environment) -> Environment:
        for d_in, e_out in zip(self.data_names, self.env_names):
            env[e_out] = env[e_out] - env[d_in]

        return env
