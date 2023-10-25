from typing import Any

from ferdelance.core.transformers.core import QueryTransformer
from ferdelance.core.queries import QueryFeature, FilterOperation

from pydantic import validator

import pandas as pd


class FederatedFilter(QueryTransformer):
    feature: QueryFeature
    operation: FilterOperation
    value: str | Any

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

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)

    def apply(self, df: pd.DataFrame) -> pd.Series:
        feature: str = self.feature.name
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

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        if X_tr:
            mask = self.apply(X_tr[self.feature])
            X_tr = X_tr[mask]
            if y_tr:
                y_tr = y_tr[mask]
        if X_ts:
            mask = self.apply(X_ts[self.feature])
            X_ts = X_ts[mask]
            if y_ts:
                y_ts = y_ts[mask]

        return X_tr, y_tr, X_ts, y_ts, None
