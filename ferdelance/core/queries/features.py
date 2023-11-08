from __future__ import annotations

from ferdelance.core.entity import Entity
from ferdelance.core.queries.operations import FilterOperation

from pandas import DataFrame, to_datetime
from datetime import datetime


def is_numeric(other) -> bool:
    return isinstance(other, int | float)


def is_string(other) -> bool:
    return isinstance(other, str)


def is_time(other) -> bool:
    return isinstance(other, datetime)


class QueryFeature(Entity):
    """Query feature to use in a query from the workbench."""

    name: str
    dtype: str | None = None

    def __init__(self, name: str, dtype: str | None = None, **data):
        super(QueryFeature, self).__init__(name=name, dtype=dtype, **data)  # type: ignore

    def _filter(self, operation: FilterOperation, value) -> QueryFilter:
        return QueryFilter(
            feature=self,
            operation=operation.name,
            value=f"{value}",
        )

    def _dtype_numeric(self) -> bool:
        return self.dtype in ("int", "float", "int64", "float64")

    def __lt__(self, other) -> QueryFilter:
        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(FilterOperation.NUM_LESS_THAN, other)

            if is_time(other):
                return self._filter(FilterOperation.TIME_BEFORE, other)

        raise ValueError('operator less than "<" can be used only for int, float, or time values')

    def __le__(self, other) -> QueryFilter:
        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(FilterOperation.NUM_LESS_EQUAL, other)

        raise ValueError('operator less equal "<=" can be used only for int or float values')

    def __gt__(self, other) -> QueryFilter:
        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(FilterOperation.NUM_GREATER_THAN, other)

            if is_time(other):
                return self._filter(FilterOperation.TIME_AFTER, other)

        raise ValueError('operator greater than ">" can be used only for int, float, or time values')

    def __ge__(self, other) -> QueryFilter:
        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(FilterOperation.NUM_GREATER_EQUAL, other)

        raise ValueError('operator greater equal ">=" can be used only for int or float values')

    def __eq__(self, other) -> bool | QueryFilter:
        if isinstance(other, QueryFeature):
            return self.name == other.name

        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(FilterOperation.NUM_EQUALS, other)

            if is_time(other):
                return self._filter(FilterOperation.TIME_EQUALS, other)

        if is_string(other):
            return self._filter(FilterOperation.OBJ_LIKE, other)

        raise ValueError('operator equals "==" can be used only for int, float, str, or time values')

    def __ne__(self, other) -> QueryFilter:
        if self._dtype_numeric():
            if is_numeric(other):
                return self._filter(FilterOperation.NUM_NOT_EQUALS, other)

            if is_time(other):
                return self._filter(FilterOperation.TIME_NOT_EQUALS, other)

        if is_string(other):
            return self._filter(FilterOperation.OBJ_NOT_LIKE, other)

        raise ValueError('operator not equals "!=" can be used only for int, float, str, or time values')

    def __hash__(self) -> int:
        return hash((self.name, self.dtype))

    def __str__(self) -> str:
        return f"{self.name}"


class QueryFilter(Entity):
    """Query filter to apply to the feature from the workbench."""

    feature: QueryFeature
    operation: str
    value: str

    def __call__(self, df: DataFrame) -> DataFrame:
        feature: str = self.feature.name
        op: FilterOperation = FilterOperation[self.operation]
        parameter: str = self.value

        if op == FilterOperation.NUM_LESS_THAN:
            return df[df[feature] < float(parameter)]
        if op == FilterOperation.NUM_LESS_EQUAL:
            return df[df[feature] <= float(parameter)]
        if op == FilterOperation.NUM_GREATER_THAN:
            return df[df[feature] > float(parameter)]
        if op == FilterOperation.NUM_GREATER_EQUAL:
            return df[df[feature] >= float(parameter)]
        if op == FilterOperation.NUM_EQUALS:
            return df[df[feature] == float(parameter)]
        if op == FilterOperation.NUM_NOT_EQUALS:
            return df[df[feature] != float(parameter)]

        if op == FilterOperation.OBJ_LIKE:
            return df[df[feature] == parameter]
        if op == FilterOperation.OBJ_NOT_LIKE:
            return df[df[feature] != parameter]

        if op == FilterOperation.TIME_BEFORE:
            return df[df[feature] < to_datetime(parameter)]
        if op == FilterOperation.TIME_AFTER:
            return df[df[feature] > to_datetime(parameter)]
        if op == FilterOperation.TIME_EQUALS:
            return df[df[feature] == to_datetime(parameter)]
        if op == FilterOperation.TIME_NOT_EQUALS:
            return df[df[feature] != to_datetime(parameter)]

        raise ValueError(f'Unsupported operation "{self.operation}" ')

    def __eq__(self, other: QueryFilter) -> bool:
        if not isinstance(other, QueryFilter):
            return False

        return self.feature == other.feature and self.operation == other.operation and self.value == other.value

    def __hash__(self) -> int:
        return hash((self.feature, self.operation, self.value))

    def __str__(self) -> str:
        return f"Filter({self.feature} {self.operation} {self.value})"
