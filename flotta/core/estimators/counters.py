from __future__ import annotations
from typing import Sequence

from flotta.core.interfaces import Step
from flotta.core.environment import Environment
from flotta.core.estimators.core import Estimator
from flotta.core.operations import Operation, QueryOperation
from flotta.core.steps import Sequential

import numpy as np


class InitCounter(Operation):
    def exec(self, env: Environment) -> Environment:
        r = np.random.default_rng(self.random_state)

        rand_value = r.integers(-(2**31), 2**31, size=1)[0]

        env[".init_value"] = rand_value
        env["count"] = rand_value

        return env


class Count(QueryOperation):
    def exec(self, env: Environment) -> Environment:
        if self.query is not None:
            env = self.query.apply(env)

        if env.df is None:
            raise ValueError("Input data not set")

        ids = env.list_resource_ids()
        if len(ids) != 1:
            raise ValueError("Count algorithm requires exactly one resource")

        r = ids[0]

        env["count"] = env[r]["count"] + env.df.shape[0]

        return env


class CleanCounter(Operation):
    def exec(self, env: Environment) -> Environment:
        ids = env.list_resource_ids()
        if len(ids) != 1:
            raise ValueError("Count algorithm requires exactly one resource")

        r = ids[0]

        env["count"] = env[r]["count"] - env[".init_value"]

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
