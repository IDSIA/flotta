from __future__ import annotations

from ferdelance_shared.operations import Operations
from ferdelance_shared.schemas import (
    Feature as BaseFeature,
    QueryFilter as BaseQueryFilter,
    QueryFeature as BaseQueryFeature,
    QueryTransformer as BaseQueryTransformer,
    Query as BaseQuery,
)

from datetime import datetime


class QueryFilter(BaseQueryFilter):

    def __eq__(self, other: QueryFilter) -> bool:
        if not isinstance(other, QueryFilter):
            return False

        return (
            self.feature == other.feature and
            self.operation == other.operation and
            self.parameter == other.parameter
        )

    def __hash__(self) -> int:
        return hash((self.feature, self.operation, self.parameter))


class QueryTransformer(BaseQueryTransformer):

    def __eq__(self, other: QueryTransformer) -> bool:
        if not isinstance(other, QueryTransformer):
            return False

        return (
            self.feature == other.feature and
            self.name == other.name
        )

    def __hash__(self) -> int:
        return hash((self.feature, self.name))


class QueryFeature(BaseQueryFeature):

    def __eq__(self, other: QueryFeature | Feature) -> bool:
        if not isinstance(other, QueryFeature | Feature):
            return False

        if isinstance(other, Feature):
            other = other.qf()

        return self.datasource_id == other.datasource_id and self.feature_id == other.feature_id

    def __hash__(self) -> int:
        return hash((self.datasource_id, self.feature_id))


class Feature(BaseFeature):

    def _filter(self, operation: Operations, value):
        return QueryFilter(
            feature=self.qf(),
            operation=operation.name,
            parameter=f'{value}',
        )

    def qf(self) -> QueryFeature:
        return QueryFeature(
            feature_id=self.feature_id,
            datasource_id=self.datasource_id,
            feature_name=self.name,
            datasource_name=self.datasource_name,
        )

    def info(self) -> str:
        lines: list[str] = [
            f'{self.name}',
            f'Data type:            {self.dtype}',
        ]

        if self.dtype != 'object':
            lines += [
                f'Value min:            {self.v_min:.2f}',
                f'Value max:            {self.v_max:.2f}',
                f'Mean:                 {self.v_mean:.2f}',
                f'Std deviation:        {self.v_std:.2f}',
                f'Value 25° percentile: {self.v_p25:.2f}',
                f'Value 50° percentile: {self.v_p50:.2f}',
                f'Value 75° percentile: {self.v_p75:.2f}',
                f'Missing value:        {self.v_miss:.2f}',
            ]

        return '\n'.join(lines)

    def __lt__(self, other) -> QueryFilter:
        if self.dtype == 'int' and self.dtype == 'float':

            if isinstance(other, int | float):
                return self._filter(Operations.NUM_LESS_THAN, other)

            if isinstance(other, datetime):
                return self._filter(Operations.TIME_BEFORE, other)

        raise ValueError('operator less than "<" can be used only for int, float, or time values')

    def __le__(self, other) -> QueryFilter:
        if isinstance(other, int | float):
            return self._filter(Operations.NUM_LESS_EQUAL, other)

        raise ValueError('operator less equal "<=" can be used only for int or float values')

    def __gt__(self, other) -> QueryFilter:
        if isinstance(other, int | float):
            return self._filter(Operations.NUM_GREATER_THAN, other)

        if isinstance(other, datetime):
            return self._filter(Operations.TIME_AFTER, other)

        raise ValueError('operator greater than ">" can be used only for int, float, or time values')

    def __ge__(self, other) -> QueryFilter:
        if isinstance(other, int | float):
            return self._filter(Operations.NUM_GREATER_EQUAL, other)

        raise ValueError('operator greater equal ">=" can be used only for int or float values')

    def __eq__(self, other) -> bool | QueryFilter:
        if isinstance(other, Feature):
            return (
                self.datasource_id == other.datasource_id and
                self.datasource_name == other.datasource_name and
                self.feature_id == other.feature_id and
                self.name == other.name and
                self.dtype == other.dtype
            )

        if isinstance(other, int | float):
            return self._filter(Operations.NUM_EQUALS, other)

        if isinstance(other, str):
            return self._filter(Operations.OBJ_LIKE, other)

        if isinstance(other, datetime):
            return self._filter(Operations.TIME_EQUALS, other)

        raise ValueError('operator equals "==" can be used only for int, float, str, or time values')

    def __ne__(self, other) -> QueryFilter:
        if isinstance(other, int | float):
            return self._filter(Operations.NUM_NOT_EQUALS, other)

        if isinstance(other, str):
            return self._filter(Operations.OBJ_NOT_LIKE, other)

        if isinstance(other, datetime):
            return self._filter(Operations.TIME_NOT_EQUALS, other)

        raise ValueError('operator not equals "!=" can be used only for int, float, str, or time values')

    def __hash__(self) -> int:
        return hash((self.datasource_id, self.feature_id, self.name, self.dtype))


class Query(BaseQuery):

    features: list[QueryFeature] = list()

    def copy(self) -> Query:
        return Query(
            datasource_id=self.datasource_id,
            datasource_name=self.datasource_name,
            features=self.features.copy(),
            filters=self.filters.copy(),
            transformers=self.transformers.copy()
        )

    def add_feature(self, feature: Feature | QueryFeature) -> None:
        if isinstance(feature, Feature):
            feature = feature.qf()

        if feature.datasource_id != self.datasource_id:
            raise ValueError('Cannot add features from a different data source')

        if feature not in self.features:
            self.features.append(feature)

    def add_filter(self, filter: QueryFilter) -> None:
        if filter.feature.datasource_id != self.datasource_id:
            raise ValueError('Cannot add filter for features from a different data source')

        self.filters.append(filter)

    def __add__(self, other: Feature | QueryFeature | QueryFilter) -> Query:
        if isinstance(other, Feature | QueryFeature):
            q = self.copy()
            q.add_feature(other)
            return q

        if isinstance(other, QueryFilter):
            q = self.copy()
            q.add_filter(other)
            return q

        raise ValueError('only Feature or QueryFeature objects can be added to Query objects')

    def __iadd__(self, other: QueryFeature | Feature | QueryFilter) -> Query:
        if isinstance(other, Feature | QueryFeature):
            self.add_feature(other)
            return self

        if isinstance(other, QueryFilter):
            self.add_filter(other)
            return self

        raise ValueError('only Feature or QueryFeature objects can be added to Query objects')

    def remove_feature(self, feature: Feature | QueryFeature) -> None:
        if isinstance(feature, Feature):
            feature = feature.qf()

        if feature.datasource_id != self.datasource_id:
            raise ValueError('Cannot remove features from a different data source')

        self.features = [f for f in self.features if f != feature]
        self.filters = [f for f in self.filters if f.feature != feature]
        self.transformers = [f for f in self.transformers if f.feature != feature]

    def remove_filter(self, filter: QueryFilter) -> None:
        if filter.feature.datasource_id != self.datasource_id:
            raise ValueError('Cannot remove filter for features from a different data source')

        self.filters.remove(filter)

    def __sub__(self, other: QueryFeature | Feature) -> Query:
        if isinstance(other, Feature | QueryFeature):
            q = self.copy()
            q.remove_feature(other)
            return q

        if isinstance(other, QueryFilter):
            q = self.copy()
            q.remove_filter(other)
            return q

        raise ValueError('only Feature or QueryFeature objects can be removed from Query objects')

    def __isub__(self, other: QueryFeature | Feature) -> Query:
        if isinstance(other, Feature | QueryFeature):
            self.remove_feature(other)
            return self

        if isinstance(other, QueryFilter):
            self.remove_filter(other)
            return self

        raise ValueError('only Feature or QueryFeature objects can be removed from Query objects')

    def __eq__(self, other: Query) -> bool:
        if not isinstance(other, Query):
            return False

        return (
            self.datasource_id == other.datasource_id and
            self.datasource_name == other.datasource_name and
            self.features == other.features and
            self.filters == other.filters and
            self.transformers == other.transformers
        )

    def __hash__(self) -> int:
        return hash((self.datasource_id, self.features, self.filters, self.transformers))

    def __getitem__(self, key: QueryFilter) -> Query:
        if isinstance(key, QueryFilter):
            return self + key

        raise ValueError('unsupported key type')
