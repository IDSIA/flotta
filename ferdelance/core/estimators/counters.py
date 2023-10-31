from __future__ import annotations
from typing import Sequence

from ferdelance.core.interfaces import Step
from ferdelance.core.environment import Environment
from ferdelance.core.estimators.core import Estimator
from ferdelance.core.operations import Operation, QueryOperation
from ferdelance.core.steps import Sequential

import numpy as np


class InitCounter(Operation):
    def exec(self, env: Environment) -> Environment:
        r = np.random.default_rng(self.random_state)

        rand_value = r.integers(-(2**31), 2**31, size=1)[0]

        env["_init_value"] = rand_value
        env["count"] = rand_value

        return env


class Count(QueryOperation):
    def exec(self, env: Environment) -> Environment:
        if self.query is not None:
            env = self.query.apply(env)

        if env.X_tr is None:
            raise ValueError("Input data not set")

        env["count"] += env.X_tr.shape[0]

        return env


class CleanCounter(Operation):
    def exec(self, env: Environment) -> Environment:
        rand_value = env["_init_value"]

        env["count"] -= rand_value

        return env


class CountEstimator(Estimator):
    def get_steps(self) -> Sequence[Step]:
        return [
            Sequential(
                init_operation=InitCounter(),
                operation=Count(query=self.query),
                final_operation=CleanCounter(),
            ),
        ]
