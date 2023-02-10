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

    features_in: QueryFeature | list[QueryFeature] | str | list[str]
    features_out: QueryFeature | list[QueryFeature] | str | list[str]
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

    def _filter(self, operation: Operations, value):
        return QueryFilter(
            feature=self,
            operation=operation.name,
            parameter=f"{value}",
        )

    def _dtype_numeric(self):
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


class Query(BaseModel):
    """Query to apply to the selected data from the workbench."""

    datasource_id: str
    datasource_name: str
    features: list[QueryFeature] = list()
    filters: list[QueryFilter] = list()
    transformers: list[QueryTransformer] = list()

    def copy(self) -> Query:
        return Query(
            datasource_id=self.datasource_id,
            datasource_name=self.datasource_name,
            features=self.features.copy(),
            filters=self.filters.copy(),
            transformers=self.transformers.copy(),
        )

    def add_feature(self, feature: QueryFeature) -> None:
        if feature not in self.features:
            self.features.append(feature)

    def add_filter(self, filter: QueryFilter) -> None:
        self.filters.append(filter)

    def add_transformer(self, transformer: QueryTransformer) -> None:
        self.transformers.append(transformer)

    def __add__(self, other: QueryFeature | QueryFilter | QueryTransformer) -> Query:
        if isinstance(other, QueryFilter):
            q = self.copy()
            q.add_filter(other)
            return q

        if isinstance(other, QueryTransformer):
            q = self.copy()
            q.add_transformer(other)
            return q

        raise ValueError(
            "Only Feature, QueryFeature, QueryFilter, or QueryTransformer objects can be added to Query objects"
        )

    def __iadd__(self, other: QueryFeature | QueryFilter) -> Query:
        if isinstance(other, QueryFilter):
            self.add_filter(other)
            return self

        if isinstance(other, QueryTransformer):
            self.add_transformer(other)
            return self

        raise ValueError(
            "Only Feature, QueryFeature, QueryFilter, or QueryTransformer objects can be added to Query objects"
        )

    def remove_feature(self, feature: QueryFeature) -> None:
        self.features = [f for f in self.features if f != feature]
        self.filters = [f for f in self.filters if f.feature != feature]
        self.transformers = [f for f in self.transformers if f.features_in != feature]

    def remove_filter(self, filter: QueryFilter) -> None:
        self.filters.remove(filter)

    def __sub__(self, other: QueryFeature) -> Query:
        if isinstance(other, QueryFeature):
            q = self.copy()
            q.remove_feature(other)
            return q

        if isinstance(other, QueryFilter):
            q = self.copy()
            q.remove_filter(other)
            return q

        raise ValueError("only Feature or QueryFeature objects can be removed from Query objects")

    def __isub__(self, other: QueryFeature) -> Query:
        if isinstance(other, QueryFeature):
            self.remove_feature(other)
            return self

        if isinstance(other, QueryFilter):
            self.remove_filter(other)
            return self

        raise ValueError("only Feature or QueryFeature objects can be removed from Query objects")

    def __eq__(self, other: Query) -> bool:
        if not isinstance(other, Query):
            return False

        return (
            self.datasource_id == other.datasource_id
            and self.datasource_name == other.datasource_name
            and self.features == other.features
            and self.filters == other.filters
            and self.transformers == other.transformers
        )

    def __hash__(self) -> int:
        return hash((self.datasource_id, self.features, self.filters, self.transformers))

    def __getitem__(self, key: QueryFilter) -> Query:
        if isinstance(key, QueryFilter):
            return self + key

        raise ValueError("unsupported key type")
