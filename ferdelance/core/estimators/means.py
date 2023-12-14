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

        env[".init_sum"] = vals[0]
        env[".init_count"] = vals[1]

        env["sum"] = vals[0]
        env["count"] = vals[1]

        return env


class Mean(QueryOperation):
    def exec(self, env: Environment) -> Environment:
        if self.query is not None:
            env = self.query.apply(env)

        if env.df is None:
            raise ValueError("Input data not set")

        ids = env.list_resource_ids()
        if len(ids) != 1:
            raise ValueError("Mean algorithm requires exactly one resource")

        r = ids[0]

        env["sum"] = env[r]["sum"] + env.df.sum(axis=1)
        env["count"] = env[r]["count"] + env.df.shape[0]

        return env


class CleanMean(Operation):
    def exec(self, env: Environment) -> Environment:
        ids = env.list_resource_ids()
        if len(ids) != 1:
            raise ValueError("Mean algorithm requires exactly one resource")

        r = ids[0]

        sum = env[r]["sum"] - env[".init_sum"]
        count = env[r]["count"] - env[".init_count"]

        env["mean"] = 1.0 * sum / count

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
