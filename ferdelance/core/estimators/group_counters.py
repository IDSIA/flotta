from __future__ import annotations
from typing import Any, Sequence
from ferdelance.core.environment import Environment

from ferdelance.core.estimators.core import Estimator
from ferdelance.core.interfaces import Step

import numpy as np
from ferdelance.core.operations.core import Operation, QueryOperation

from ferdelance.core.steps import Sequential


class InitGroupCounter(Operation):
    def exec(self, env: Environment) -> Environment:
        r = np.random.default_rng(self.random_state)

        rand_value = r.integers(-(2**31), 2**31, size=1)[0]

        env[".init_value"] = rand_value
        env["noise"] = rand_value

        return env


class GroupCount(QueryOperation):
    by: list[str]
    features: list[str]

    def exec(self, env: Environment) -> Environment:
        if self.query is not None:
            env = self.query.apply(env)

        if env.df is None:
            raise ValueError("Input data not set")

        ids = env.list_resource_ids()
        if len(ids) != 1:
            raise ValueError("Count algorithm requires exactly one resource")

        r = ids[0]

        noise = env[r]["noise"]
        counts_in = env[r]["counts"]
        count_out = dict()

        group_count = env.df.groupby(self.by).count().to_dict()

        for feature in self.features:
            counts: dict[str, Any] = group_count[feature]

            count_out[feature] = dict()

            for k in counts.keys():
                count_out[feature][k] = counts[k] + counts_in[feature].get(k, 0) + noise

        env["counts"] = count_out

        if noise != 0:
            env["noise"] = 0

        return env


class CleanGroupCounter(Operation):
    def exec(self, env: Environment) -> Environment:
        ids = env.list_resource_ids()
        if len(ids) != 1:
            raise ValueError("Count algorithm requires exactly one resource")

        r = ids[0]

        counts_in = env[r]["counts"]
        counts_out = dict()

        for feature in counts_in.keys():
            counts_out[feature] = dict()

            for k in counts_in[feature].keys():
                counts_out[feature][k] = counts_in[feature][k] - env[".init_value"]

        env["counts"] = counts_out

        return env


class GroupCountEstimator(Estimator):
    # columns to group by
    by: list[str]
    features: list[str]

    def get_steps(self) -> Sequence[Step]:
        return [
            Sequential(
                init_operation=InitGroupCounter(),
                operation=GroupCount(
                    by=self.by,
                    features=self.features,
                    query=self.query,
                ),
                final_operation=CleanGroupCounter(),
            )
        ]
