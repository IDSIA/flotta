from typing import Any

from flotta.core.environment import Environment
from flotta.core.transformers.core import QueryTransformer

from pydantic import field_validator
from sklearn.impute import SimpleImputer

import numpy as np


class FederatedSimpleImputer(QueryTransformer):
    missing_values: float = np.nan
    strategy: str = "mean"
    fill_value: Any = None

    @field_validator("fill_value")
    def set_strategy_from_fill_value(cls, values: dict[str, Any]):
        if not values.get("fill_value", None):
            values["strategy"] = "constant"
        return values

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        if env.X_tr is None:
            raise ValueError("X_tr required!")

        tr = SimpleImputer(
            missing_values=self.missing_values,
            strategy=self.strategy,
            fill_value=self.fill_value,
        )

        tr.fit(env.X_tr[self._columns_in()], env.Y_tr)

        if env.X_ts is None:
            X = env.X_tr
        else:
            X = env.X_ts

        X[self._columns_out()] = tr.transform(X[self._columns_in()])

        return env, tr

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        raise NotImplementedError()
