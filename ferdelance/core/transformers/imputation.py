from typing import Any

from ferdelance.core.transformers.core import QueryTransformer

from pydantic import validator
from sklearn.impute import SimpleImputer

import pandas as pd
import numpy as np


class FederatedSimpleImputer(QueryTransformer):
    missing_values: float = np.nan
    strategy: str = "mean"
    fill_value: Any = None

    @validator("fill_value")
    def set_strategy_from_fill_value(cls, values: dict[str, Any]):
        if not values.get("fill_value", None):
            values["strategy"] = "constant"
        return values

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        tr = SimpleImputer(
            missing_values=self.missing_values,
            strategy=self.strategy,
            fill_value=self.fill_value,
        )

        if X_tr is None:
            raise ValueError("X_tr required!")

        if X_ts is None:
            X_ts = X_tr
        else:
            X_ts = X_ts

        tr.fit(X_tr[self._columns_in()], y_tr)
        X_ts[self._columns_out()] = tr.transform(X_ts[self._columns_in()])

        return X_tr, y_tr, X_ts, y_ts, tr

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)
