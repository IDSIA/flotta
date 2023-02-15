from __future__ import annotations

from typing import Any

from .operations import Operations

from datetime import datetime
from pandas import DataFrame, to_datetime
from pydantic import BaseModel


def is_numeric(other) -> bool:
    return isinstance(other, int | float)


def is_string(other) -> bool:
    return isinstance(other, str)


def is_time(other) -> bool:
    return isinstance(other, datetime)


class QueryFilter(BaseModel):
    """Query filter to apply to the feature from the workbench."""

    feature: QueryFeature
    operation: str
    parameter: str

    def __call__(self, df: DataFrame) -> DataFrame:
        feature: str = self.feature.feature_name
        op: Operations = Operations[self.operation]
        parameter: str = self.parameter

        if op == Operations.NUM_LESS_THAN:
            return df[df[feature] < float(parameter)]
        if op == Operations.NUM_LESS_EQUAL:
            return df[df[feature] <= float(parameter)]
        if op == Operations.NUM_GREATER_THAN:
            return df[df[feature] > float(parameter)]
        if op == Operations.NUM_GREATER_EQUAL:
            return df[df[feature] >= float(parameter)]
        if op == Operations.NUM_EQUALS:
            return df[df[feature] == float(parameter)]
        if op == Operations.NUM_NOT_EQUALS:
            return df[df[feature] != float(parameter)]

        if op == Operations.OBJ_LIKE:
            return df[df[feature] == parameter]
        if op == Operations.OBJ_NOT_LIKE:
            return df[df[feature] != parameter]

        if op == Operations.TIME_BEFORE:
            return df[df[feature] < to_datetime(parameter)]
        if op == Operations.TIME_AFTER:
            return df[df[feature] > to_datetime(parameter)]
        if op == Operations.TIME_EQUALS:
            return df[df[feature] == to_datetime(parameter)]
        if op == Operations.TIME_NOT_EQUALS:
            return df[df[feature] != to_datetime(parameter)]

        raise ValueError(f'Unsupported operation "{self.operation}" ')

    def __eq__(self, other: QueryFilter) -> bool:
        if not isinstance(other, QueryFilter):
            return False

        return self.feature == other.feature and self.operation == other.operation and self.parameter == other.parameter

    def __hash__(self) -> int:
        return hash((self.feature, self.operation, self.parameter))

    def __str__(self) -> str:
        return f"Filter({self.feature} {self.operation} {self.parameter})"


class QueryTransformer(BaseModel):
    """Query transformation to apply to the feature from the workbench."""

    features_in: list[QueryFeature]
    features_out: list[QueryFeature]
    name: str
    parameters: dict[str, Any]

    def params(self) -> dict[str, Any]:
        return {
            "features_in": self.features_in,
            "features_out": self.features_out,
        } | self.parameters

    def __eq__(self, other: QueryTransformer) -> bool:
        if not isinstance(other, QueryTransformer):
            return False

        return (
            self.features_in == other.features_in
            and self.features_out == other.features_out
            and self.name == other.name
        )

    def __hash__(self) -> int:
        return hash((self.features_in, self.features_out, self.name))

    def __str__(self) -> str:
        return f"{self.name}({self.features_in} -> {self.features_out})"


class QueryFeature(BaseModel):
    """Query feature to use in a query from the workbench."""

    feature_name: str

    dtype: str | None

    def _filter(self, operation: Operations, value) -> QueryFilter:
        return QueryFilter(
            feature=self,
            operation=operation.name,
            parameter=f"{value}",
        )

    def _dtype_numeric(self) -> bool:
        return self.dtype in ("int", "float", "int64", "float64")

    def __lt__(self, other) -> QueryFilter:
        if self._dtype_numeric():

            if is_numeric(other):
                return self._filter(Operations.NUM_LESS_THAN, other)

            if is_time(other):
                return self._filter(Operations.TIME_BEFORE, other)

        raise ValueError('operator less than "<" can be used only for int, float, or time values')

    def __le__(self, other) -> QueryFilter:
        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(Operations.NUM_LESS_EQUAL, other)

        raise ValueError('operator less equal "<=" can be used only for int or float values')

    def __gt__(self, other) -> QueryFilter:
        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(Operations.NUM_GREATER_THAN, other)

            if is_time(other):
                return self._filter(Operations.TIME_AFTER, other)

        raise ValueError('operator greater than ">" can be used only for int, float, or time values')

    def __ge__(self, other) -> QueryFilter:
        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(Operations.NUM_GREATER_EQUAL, other)

        raise ValueError('operator greater equal ">=" can be used only for int or float values')

    def __eq__(self, other) -> bool | QueryFilter:
        if isinstance(other, QueryFeature):
            return self.feature_name == other.feature_name

        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(Operations.NUM_EQUALS, other)

            if is_time(other):
                return self._filter(Operations.TIME_EQUALS, other)

        if is_string(other):
            return self._filter(Operations.OBJ_LIKE, other)

        raise ValueError('operator equals "==" can be used only for int, float, str, or time values')

    def __ne__(self, other) -> QueryFilter:
        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(Operations.NUM_NOT_EQUALS, other)

            if is_time(other):
                return self._filter(Operations.TIME_NOT_EQUALS, other)

        if is_string(other):
            return self._filter(Operations.OBJ_NOT_LIKE, other)

        raise ValueError('operator not equals "!=" can be used only for int, float, str, or time values')

    def __hash__(self) -> int:
        return hash((self.feature_name, self.dtype))

    def __str__(self) -> str:
        return f"{self.feature_name}"


class QueryStage(BaseModel):
    """A stage is a single transformation applied to a list of features."""

    features: list[QueryFeature]  # list of available features (after the transformation below)
    transformer: QueryTransformer | None = None  # transformation to apply

    _features: dict[str, QueryFeature] = dict()

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

        self._features = {f.feature_name: f for f in self.features}

    def __getitem__(self, key: str | QueryFeature) -> QueryFeature:
        if isinstance(key, QueryFeature):
            key = key.feature_name

        if key not in self._features:
            raise ValueError(f"feature {key} not found")

        return self._features[key]


class Query(BaseModel):
    """A query is just a list of stages applied to the input data."""

    stages: list[QueryStage] = list()

    def copy(self) -> Query:
        # this is a shallow copy!
        return Query(stages=self.stages.copy())

    def features(self) -> list[QueryFeature]:
        return self.stages[-1].features

    def feature(self, key: str | QueryFeature) -> QueryFeature:
        if isinstance(key, QueryFeature):
            key = key.feature_name

        return self.stages[-1][key]

    def add_transformer(self, transformer: QueryTransformer) -> None:
        fs = [f for f in self.features() if f not in transformer.features_in]
        fs += transformer.features_out

        self.stages.append(
            QueryStage(
                features=fs,
                transformer=transformer,
            )
        )

    def __add__(self, other: QueryFeature | QueryTransformer) -> Query:
        if isinstance(other, QueryTransformer):
            q = self.copy()
            q.add_transformer(other)
            return q

        raise ValueError(
            "Only Feature, QueryFeature, QueryFilter, or QueryTransformer objects can be added to Query objects"
        )

    def __iadd__(self, other: QueryFeature | QueryTransformer) -> Query:
        if isinstance(other, QueryTransformer):
            self.add_transformer(other)
            return self

        raise ValueError(
            "Only Feature, QueryFeature, QueryFilter, or QueryTransformer objects can be added to Query objects"
        )

    def __getitem__(self, key: str | QueryFeature) -> QueryFeature:

        if isinstance(key, QueryFeature):
            key = key.feature_name

        return self.stages[-1][key]

    def __eq__(self, other: Query) -> bool:
        if not isinstance(other, Query):
            return False

        return self.stages == other.stages

    def __hash__(self) -> int:
        return hash(self.stages)
