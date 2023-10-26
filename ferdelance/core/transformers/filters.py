from typing import Any

from ferdelance.core.environment import Environment
from ferdelance.core.transformers.core import QueryTransformer
from ferdelance.core.queries import QueryFeature, FilterOperation

from pydantic import validator

import pandas as pd


class FederatedFilter(QueryTransformer):
    feature: str
    operation: FilterOperation
    value: str

    def __init__(self, feature: str | QueryFeature, operation: FilterOperation, value: Any, **data) -> None:
        super().__init__(**data)

        if isinstance(feature, QueryFeature):
            self.feature: str = feature.name
        else:
            self.feature: str = feature

        self.operation = operation
        self.value = value

    @validator("feature")
    def validate_feature(cls, values):
        feature = values["feature"]

        if isinstance(feature, QueryFeature):
            values["feature"] = feature.name
        else:
            values["feature"] = feature

        return values

    @validator("operation")
    def validate_operation(cls, values):
        operation = values["operation"]

        if isinstance(operation, FilterOperation):
            values["operation"] = operation.name
        else:
            values["operation"] = operation

        return values

    @validator("value")
    def validate_value(cls, values):
        values["value"] = f"{values['value']}"
        return values

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        return super().aggregate(env)

    def apply(self, df: pd.DataFrame) -> pd.Series:
        feature: str = self.feature
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
            if env.y_tr:
                env.y_tr = env.y_tr[mask]

        if env.X_ts:
            mask = self.apply(env.X_ts)
            env.X_ts = env.X_ts[mask]
            if env.y_ts:
                env.y_ts = env.y_ts[mask]

        return env, None
