from typing import Any

from ferdelance.core.environment import Environment
from ferdelance.core.transformers.core import QueryTransformer

from sklearn.preprocessing import (
    MinMaxScaler,
    StandardScaler,
)

from pydantic import validator


class FederatedMinMaxScaler(QueryTransformer):
    """Wrapper of scikit-learn MinMaxScaler.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MinMaxScaler.html
    """

    feature_range: tuple[int, int] = (0, 1)

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        tr = MinMaxScaler(
            feature_range=self.feature_range,
        )

        c_in = self._columns_in()
        c_out = self._columns_out()

        if env.X_tr is None:
            raise ValueError("X_tr required!")

        tr.fit(env.X_tr[c_in])

        if env.X_ts is None:
            X = env.X_tr
        else:
            X = env.X_ts

        X[c_out] = tr.transform(X[c_in])

        return env, tr

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        return super().aggregate(env)


class FederatedStandardScaler(QueryTransformer):
    """Wrapper of scikit-learn StandardScaler.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html
    """

    with_mean: bool = True
    with_std: bool = True

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        tr = StandardScaler(
            with_mean=self.with_mean,
            with_std=self.with_std,
        )

        if env.X_tr is None:
            raise ValueError("X_tr required!")

        tr.fit(env.X_tr[self._columns_in()])

        if env.X_ts is None:
            X = env.X_tr
        else:
            X = env.X_ts

        X[self._columns_out()] = tr.transform(X[self._columns_in()])

        return env, tr

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        return super().aggregate(env)


class FederatedClamp(QueryTransformer):
    """Fix the values of one or more columns to a given interval if their values are outside of the interval itself."""

    # TODO: test this!

    min_value: float | None = None
    max_value: float | None = None

    @validator("min_value", "max_value")
    def min_max_values(cls, values: dict):
        min_value = values.get("min_value", None)
        max_value = values.get("max_value", None)

        if min_value is not None and max_value is not None and min_value > max_value:
            min_value, max_value = max_value, min_value

        values["min_value"] = min_value
        values["max_value"] = max_value
        return values

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        c_in = self._columns_in()
        c_out = self._columns_out()

        if self.min_value is not None:
            if env.X_tr is not None:
                env.X_tr[c_out] = env.X_tr[c_in].where(env.X_tr[c_in] < self.min_value, self.min_value)
            if env.X_ts is not None:
                env.X_ts[c_out] = env.X_ts[c_in].where(env.X_ts[c_in] < self.min_value, self.min_value)

        if self.max_value is not None:
            if env.X_tr is not None:
                env.X_tr[c_out] = env.X_tr[c_in].where(env.X_tr[c_in] > self.max_value, self.max_value)
            if env.X_ts is not None:
                env.X_ts[c_out] = env.X_ts[c_in].where(env.X_ts[c_in] > self.max_value, self.max_value)

        return env, None

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        return super().aggregate(env)
