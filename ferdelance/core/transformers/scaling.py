from typing import Any

from pydantic import validator

from ferdelance.core.transformers.core import QueryTransformer

from sklearn.preprocessing import (
    MinMaxScaler,
    StandardScaler,
)

import pandas as pd


class FederatedMinMaxScaler(QueryTransformer):
    """Wrapper of scikit-learn MinMaxScaler.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MinMaxScaler.html
    """

    feature_range: tuple[int, int] = (0, 1)

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        tr = MinMaxScaler(
            feature_range=self.feature_range,
        )

        c_in = self._columns_in()
        c_out = self._columns_out()

        if X_tr is None:
            raise ValueError("X_tr required!")

        if X_ts is None:
            X_ts = X_tr
        else:
            X_ts = X_ts

        tr.fit(X_tr[c_in])
        X_ts[c_out] = tr.transform(X_ts[c_in])

        return X_tr, y_tr, X_ts, y_ts, tr

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)


class FederatedStandardScaler(QueryTransformer):
    """Wrapper of scikit-learn StandardScaler.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html
    """

    with_mean: bool = True
    with_std: bool = True

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        tr = StandardScaler(
            with_mean=self.with_mean,
            with_std=self.with_std,
        )

        if X_tr is None:
            raise ValueError("X_tr required!")

        if X_ts is None:
            X_ts = X_tr
        else:
            X_ts = X_ts

        tr.fit(X_tr[self._columns_in()])
        X_ts[self._columns_out()] = tr.transform(X_ts[self._columns_in()])

        return X_tr, y_tr, X_ts, y_ts, tr

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
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

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        c_in = self._columns_in()
        c_out = self._columns_out()

        if self.min_value is not None:
            if X_tr is not None:
                X_tr[c_out] = X_tr[c_in].where(X_tr[c_in] < self.min_value, self.min_value)
            if X_ts is not None:
                X_ts[c_out] = X_ts[c_in].where(X_ts[c_in] < self.min_value, self.min_value)

        if self.max_value is not None:
            if X_tr is not None:
                X_tr[c_out] = X_tr[c_in].where(X_tr[c_in] > self.max_value, self.max_value)
            if X_ts is not None:
                X_ts[c_out] = X_ts[c_in].where(X_ts[c_in] > self.max_value, self.max_value)

        return X_tr, y_tr, X_ts, y_ts, None

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)
