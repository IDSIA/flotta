from __future__ import annotations
from typing import Any, Sequence

from ferdelance.core.environment import Environment
from ferdelance.core.estimators import Estimator
from ferdelance.core.interfaces import Step
from ferdelance.core.operations import Operation, QueryOperation
from ferdelance.core.steps import Sequential

import numpy as np


class InitGroupCounter(Operation):
    def exec(self, env: Environment) -> Environment:
        r = np.random.default_rng(self.random_state)

        rand_value = r.integers(-(2**31), 0, size=1)[0]

        env[".init_value"] = rand_value
        env["noise"] = rand_value
        env["counts"] = dict()

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

        noise: int = env[r]["noise"]
        counts_in: dict[str, Any] = env[r]["counts"]
        counts_out: dict[str, Any] = dict()

        group_count = env.df.groupby(self.by).count().to_dict()

        for feature in self.features:
            x: dict[str, int] = counts_in.get(feature, dict())
            y: dict[str, int] = group_count[feature]

            counts_out_feature = {k: x.get(k, 0) + y.get(k, 0) for k in set(x) | set(y)}

            if noise == 0:
                counts_out[feature] = counts_out_feature
            else:
                counts_out[feature] = {k: v + noise for k, v in counts_out_feature.items()}

        env["counts"] = counts_out

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
