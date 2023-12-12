from __future__ import annotations
from typing import Sequence

from ferdelance.core.environment import Environment
from ferdelance.core.estimators.core import Estimator
from ferdelance.core.interfaces import Step
from ferdelance.core.operations import Operation, QueryOperation
from ferdelance.core.steps import Sequential

import numpy as np


class InitMean(Operation):
    def exec(self, env: Environment) -> Environment:
        r = np.random.default_rng(self.random_state)

        shape = (2,)
        vals = r.integers(-(2**31), 2**31, size=shape)

        env["_init_sum"] = vals[0]
        env["_init_count"] = vals[1]

        env["sum"] = vals[0]
        env["count"] = vals[1]

        return env


class Mean(QueryOperation):
    def exec(self, env: Environment) -> Environment:
        if self.query is not None:
            env = self.query.apply(env)

        if env.X_tr is None:
            raise ValueError("Input data not set")

        env["sum"] += env.X_tr.sum(axis=1)
        env["count"] += env.X_tr.shape[0]

        return env


class CleanMean(Operation):
    def exec(self, env: Environment) -> Environment:
        env["sum"] -= env["_init_sum"]
        env["count"] -= env["_init_count"]

        env["mean"] = env["sum"] / env["count"]

        return env


class MeanEstimator(Estimator):
    def get_steps(self) -> Sequence[Step]:
        return [
            Sequential(
                init_operation=InitMean(),
                operation=Mean(query=self.query),
                final_operation=CleanMean(),
            )
        ]
