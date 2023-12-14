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

        shape = (2,)
        vals = r.integers(-(2**31), 2**31, size=shape)

        env[".init_sum"] = vals[0]
        env[".init_num"] = vals[1]

        env["noise_sum"] = vals[0]
        env["noise_num"] = vals[1]

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
            raise ValueError("Count algorithm requires exactly one resource")

        r = ids[0]

        noise_sum = env[r]["noise_sum"]
        noise_num = env[r]["noise_num"]

        sums_in = env[r]["sums"]
        nums_in = env[r]["nums"]

        sums_out = dict()
        nums_out = dict()

        group_sum = env.df.groupby(self.by).sum().to_dict()
        group_num = env.df.groupby(self.by).count().to_dict()

        for feature in self.features:
            sums: dict[str, Any] = group_sum[feature]
            nums: dict[str, Any] = group_num[feature]

            sums_out[feature] = dict()
            nums_out[feature] = dict()

            for k in sums.keys():
                sums_out[feature][k] = sums[k] + sums_in[feature].get(k, 0) + noise_sum

            for k in nums.keys():
                nums_out[feature][k] = nums[k] + nums_in[feature].get(k, 0) + noise_num

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
            raise ValueError("Count algorithm requires exactly one resource")

        r = ids[0]

        noise_sum = env[".init_sum"]
        noise_num = env[".init_sum"]

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
