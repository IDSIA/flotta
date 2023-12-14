from typing import Any

from ferdelance.core.environment import Environment
from ferdelance.core.transformers.core import QueryTransformer
from ferdelance.core.queries import QueryFeature, FilterOperation

from pydantic import validator

import pandas as pd


class FederatedFilter(QueryTransformer):
    feature: QueryFeature
    operation: FilterOperation
    value: str | Any

    @validator("value")
    def validate_value(cls, value):
        if not isinstance(value, str):
            return f"{value}"

        return value

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        return super().aggregate(env)

    def apply(self, df: pd.DataFrame) -> pd.Series:
        feature: str = self.feature if isinstance(self.feature, str) else self.feature.name
        op: FilterOperation = self.operation
        parameter: str = self.value

        if feature not in df.columns:
            # no change applied
            return pd.Series([True for _ in range(df.shape[0])])

        if op == FilterOperation.NUM_LESS_THAN:
            return df[feature] < float(parameter)
        if op == FilterOperation.NUM_LESS_EQUAL:
            return df[feature] <= float(parameter)
        if op == FilterOperation.NUM_GREATER_THAN:
            return df[feature] > float(parameter)
        if op == FilterOperation.NUM_GREATER_EQUAL:
            return df[feature] >= float(parameter)
        if op == FilterOperation.NUM_EQUALS:
            return df[feature] == float(parameter)
        if op == FilterOperation.NUM_NOT_EQUALS:
            return df[feature] != float(parameter)

        if op == FilterOperation.OBJ_LIKE:
            return df[feature] == parameter
        if op == FilterOperation.OBJ_NOT_LIKE:
            return df[feature] != parameter

        if op == FilterOperation.TIME_BEFORE:
            return df[feature] < pd.to_datetime(parameter)
        if op == FilterOperation.TIME_AFTER:
            return df[feature] > pd.to_datetime(parameter)
        if op == FilterOperation.TIME_EQUALS:
            return df[feature] == pd.to_datetime(parameter)
        if op == FilterOperation.TIME_NOT_EQUALS:
            return df[feature] != pd.to_datetime(parameter)

        raise ValueError(f'Unsupported operation "{self.operation}" ')

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        if env.X_tr:
            mask = self.apply(env.X_tr)
            env.X_tr = env.X_tr[mask]
            if env.Y_tr:
                env.Y_tr = env.Y_tr[mask]

        if env.X_ts:
            mask = self.apply(env.X_ts)
            env.X_ts = env.X_ts[mask]
            if env.Y_ts:
                env.Y_ts = env.Y_ts[mask]

        return env, None
