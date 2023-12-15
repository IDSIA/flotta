from __future__ import annotations
from typing import Any, Sequence

from ferdelance.core.environment import Environment
from ferdelance.core.estimators import Estimator
from ferdelance.core.interfaces import Step
from ferdelance.core.operations import Operation, QueryOperation
from ferdelance.core.steps import Sequential

import numpy as np


class InitGroupMean(Operation):
    def exec(self, env: Environment) -> Environment:
        r = np.random.default_rng(self.random_state)

        vals = r.integers(-(2**31), 0, size=(2,))

        env[".init_sum"] = vals[0]
        env[".init_num"] = vals[1]

        env["noise_sum"] = vals[0]
        env["noise_num"] = vals[1]

        env["sums"] = dict()
        env["nums"] = dict()

        return env


class GroupMean(QueryOperation):
    by: list[str]
    features: list[str]

    def exec(self, env: Environment) -> Environment:
        if self.query is not None:
            env = self.query.apply(env)

        if env.df is None:
            raise ValueError("Input data not set")

        ids = env.list_resource_ids()
        if len(ids) != 1:
            raise ValueError("Mean algorithm requires exactly one resource")

        r = ids[0]

        noise_sum: int = env[r]["noise_sum"]
        noise_num: int = env[r]["noise_num"]

        sums_in: dict[str, Any] = env[r]["sums"]
        nums_in: dict[str, Any] = env[r]["nums"]

        sums_out: dict[str, Any] = dict()
        nums_out: dict[str, Any] = dict()

        group_sum = env.df.groupby(self.by).sum().to_dict()
        group_num = env.df.groupby(self.by).count().to_dict()

        for feature in self.features:
            xs: dict[str, int] = sums_in.get(feature, dict())
            ys: dict[str, int] = group_sum[feature]
            xn: dict[str, int] = nums_in.get(feature, dict())
            yn: dict[str, int] = group_num[feature]

            sums_out_feature = {k: xs.get(k, 0) + ys.get(k, 0) for k in set(xs) | set(ys)}
            nums_out_feature = {k: xn.get(k, 0) + yn.get(k, 0) for k in set(xn) | set(yn)}

            if noise_sum == 0:
                sums_out[feature] = sums_out_feature
            else:
                sums_out[feature] = {k: v + noise_sum for k, v in sums_out_feature.items()}

            if noise_num == 0:
                nums_out[feature] = nums_out_feature
            else:
                nums_out[feature] = {k: v + noise_num for k, v in nums_out_feature.items()}

        env["sums"] = sums_out
        env["nums"] = nums_out

        if noise_sum != 0:
            env["noise_sum"] = 0
        if noise_num != 0:
            env["noise_num"] = 0

        return env


class CleanGroupMean(Operation):
    def exec(self, env: Environment) -> Environment:
        ids = env.list_resource_ids()
        if len(ids) != 1:
            raise ValueError("Mean algorithm requires exactly one resource")

        r = ids[0]

        noise_sum = env[".init_sum"]
        noise_num = env[".init_num"]

        sums_in = env[r]["sums"]
        nums_in = env[r]["nums"]

        means_out = dict()

        for feature in sums_in.keys():
            means_out[feature] = dict()

            for k in sums_in[feature].keys():
                means_out[feature][k] = (sums_in[feature][k] - noise_sum) / (nums_in[feature][k] - noise_num)

        env["means"] = means_out

        return env


class GroupMeanEstimator(Estimator):
    # columns to group by
    by: list[str]
    features: list[str]

    def get_steps(self) -> Sequence[Step]:
        return [
            Sequential(
                init_operation=InitGroupMean(),
                operation=GroupMean(
                    by=self.by,
                    features=self.features,
                    query=self.query,
                ),
                final_operation=CleanGroupMean(),
            )
        ]
