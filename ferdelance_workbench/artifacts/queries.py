from __future__ import annotations

from ferdelance_shared.operations import NumericOperations, TimeOperations, ObjectOperations
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

    def _filter(self, operation: NumericOperations | TimeOperations | ObjectOperations, value):
        return QueryFilter(
            feature=self.qf(),
            operation=operation.name,
            parameter=f'{value}',
        )

    def qf(self) -> QueryFeature:
        return QueryFeature(
            feature_id=self.feature_id,
            datasource_id=self.datasource_id,
        )

    def __lt__(self, other) -> QueryFilter:
        if self.dtype == 'int' and self.dtype == 'float':

            if isinstance(other, int | float):
                return self._filter(NumericOperations.LESS_THAN, other)

            if isinstance(other, datetime):
                return self._filter(TimeOperations.BEFORE, other)

        raise ValueError('operator less than "<" can be used only for int, float, or time values')

    def __le__(self, other) -> QueryFilter:
        if isinstance(other, int | float):
            return self._filter(NumericOperations.LESS_EQUAL, other)

        raise ValueError('operator less equal "<=" can be used only for int or float values')

    def __gt__(self, other) -> QueryFilter:
        if isinstance(other, int | float):
            return self._filter(NumericOperations.GREATER_THAN, other)

        if isinstance(other, datetime):
            return self._filter(TimeOperations.AFTER, other)

        raise ValueError('operator greater than ">" can be used only for int, float, or time values')

    def __ge__(self, other) -> QueryFilter:
        if isinstance(other, int | float):
            return self._filter(NumericOperations.GREATER_EQUAL, other)

        raise ValueError('operator greater equal ">=" can be used only for int or float values')

    def __eq__(self, other) -> QueryFilter:
        if isinstance(other, Feature):
            return (
                self.datasource_id == other.datasource_id and
                self.feature_id == other.feature_id and
                self.name == other.name and
                self.dtype == other.dtype
            )

        if isinstance(other, int | float):
            return self._filter(NumericOperations.EQUALS, other)

        if isinstance(other, str):
            return self._filter(ObjectOperations.LIKE, other)

        if isinstance(other, datetime):
            return self._filter(TimeOperations.EQUALS, other)

        raise ValueError('operator equals "==" can be used only for int, float, str, or time values')

    def __ne__(self, other) -> QueryFilter:
        if isinstance(other, int | float):
            return self._filter(NumericOperations.NOT_EQUALS, other)

        if isinstance(other, str):
            return self._filter(ObjectOperations.NOT_LIKE, other)

        if isinstance(other, datetime):
            return self._filter(TimeOperations.NOT_EQUALS, other)

        raise ValueError('operator not equals "!=" can be used only for int, float, str, or time values')

    def __hash__(self) -> int:
        return hash((self.datasource_id, self.feature_id, self.name, self.dtype))


class Query(BaseQuery):

    features: list[QueryFeature] = list()

    def copy(self) -> Query:
        return Query(
            datasources_id=self.datasources_id,
            features=self.features.copy(),
            filters=self.filters.copy(),
            transformers=self.transformers.copy()
        )

    def add_feature(self, feature: Feature | QueryFeature) -> None:
        if isinstance(feature, Feature):
            feature = feature.qf()

        if feature.datasource_id != self.datasources_id:
            raise ValueError('Cannot add features from a different data source')

        if feature not in self.features:
            self.features.append(feature)

    def add_filter(self, filter: QueryFilter) -> None:
        if filter.feature.datasource_id != self.datasources_id:
            raise ValueError('Cannot add filter for features from a different data source')

        self.filters.append(filter)

    def __add__(self, other: Feature | QueryFeature | QueryFilter) -> Query:
        if isinstance(other, Feature | QueryFeature):
            q = self.copy()
            q.add_feature(other)
            return q

        if isinstance(other, QueryFilter):
            q = self.copy()
            q.add_filter(filter)
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

        if feature.datasource_id != self.datasources_id:
            raise ValueError('Cannot remove features from a different data source')

        self.features = [f for f in self.features if f != feature]
        self.filters = [f for f in self.filters if f.feature != feature]
        self.transformers = [f for f in self.transformers if f.feature != feature]

    def remove_filter(self, filter: QueryFilter) -> None:
        if filter.feature.datasource_id != self.datasources_id:
            raise ValueError('Cannot remove filter for features from a different data source')

        self.features.remove(filter)

    def __sub__(self, other: QueryFeature | Feature) -> Query:
        if isinstance(other, Feature | QueryFeature):
            q = self.copy()
            q.remove_feature(other)
            return q

        if isinstance(other, QueryFilter):
            q = self.copy()
            q.remove_filter(filter)
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
            self.datasources_id == other.datasources_id and
            self.features == other.features and
            self.filters == other.filters and
            self.transformers == other.transformers
        )

    def __hash__(self) -> int:
        return hash((self.datasources_id, self.features, self.filters, self.transformers))
