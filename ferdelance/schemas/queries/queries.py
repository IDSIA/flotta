from __future__ import annotations

from ferdelance.schemas.queries.features import QueryFeature, QueryFilter, Operations
from ferdelance.schemas.queries.stages import QueryStage, QueryEstimator, QueryTransformer
from ferdelance.schemas.estimators import CountEstimator, MeanEstimator, GroupingQuery
from ferdelance.schemas.transformers import Transformer, FederatedFilter

from datetime import datetime
from pydantic import BaseModel


def is_numeric(other) -> bool:
    return isinstance(other, int | float)


def is_string(other) -> bool:
    return isinstance(other, str)


def is_time(other) -> bool:
    return isinstance(other, datetime)


class Query(BaseModel):
    """A query is just a list of stages applied to the input data. It keeps track
    of the available features and all the transformation that need to be applied
    to the data.
    """

    stages: list[QueryStage] = list()

    def current(self) -> QueryStage:
        """Returns the most recent stage of the query.

        Returns:
            QueryStage:
                The last added and most recent stage in the Query.
        """
        return self.stages[-1]

    def features(self) -> list[QueryFeature]:
        s: QueryStage = self.current()
        return s.features

    def feature(self, key: str | QueryFeature) -> QueryFeature:
        if isinstance(key, QueryFeature):
            key = key.name

        return self.current()[key]

    def groupby(self, feature: QueryFeature | str) -> GroupingQuery:
        if isinstance(feature, str):
            feature = self[feature]

        return GroupingQuery(feature, self)

    def count(self) -> Query:
        self.add_estimator(CountEstimator().build())
        return self

    def mean(self, feature: QueryFeature | str) -> Query:
        if isinstance(feature, str):
            feature = self[feature]

        self.add_estimator(MeanEstimator().build())
        return self

    def add_estimator(self, estimator: QueryEstimator) -> None:
        self.stages.append(
            QueryStage(
                features=self.features(),
                transformer=estimator,
            )
        )

    def add_transformer(self, transformer: QueryTransformer) -> None:
        fs = [f for f in self.features() if f not in transformer.features_in]
        fs += transformer.features_out

        self.stages.append(
            QueryStage(
                features=fs,
                transformer=transformer,
            )
        )

    def add_filter(self, filter: QueryFilter) -> None:
        self.stages.append(
            QueryStage(
                features=self.features(),
                transformer=FederatedFilter(
                    feature=filter.feature,
                    operation=Operations[filter.operation],
                    value=filter.value,
                ).build(),
            )
        )

    def append(self, op: QueryFilter | QueryTransformer | Transformer) -> None:
        """Add a new operation creating a new stage. This method append the new
        stage to the list of stage of the current object, without creating a new
        one.

        Args:
            op (QueryFilter | QueryTransformer | Transformer):
                Content of the new stage to append.

        Raises:
            ValueError:
                If the argument op is not an instance of the supported class.
        """
        if isinstance(op, Transformer):
            op = op.build()

        if isinstance(op, QueryTransformer):
            self.add_transformer(op)
            return

        if isinstance(op, QueryFilter):
            self.add_filter(op)
            return

        raise ValueError(f"Unsupported type for query with input op={op}")

    def add(self, op: QueryFilter | QueryTransformer | Transformer) -> Query:
        """Add a new operation creating a new stages. The return is a _new_ Query
        object with the new stage.

        Args:
            op (QueryFilter | QueryTransformer | Transformer):
                Content of the new stage to add.

        Raises:
            ValueError:
                If the argument op is not an instance of the supported class.

        Returns:
            Query:
                A copy of the original Query object with the new state.
        """
        if isinstance(op, Transformer):
            op = op.build()

        q = self.copy(deep=True)

        if isinstance(op, QueryTransformer):
            q.add_transformer(op)
            return q

        if isinstance(op, QueryFilter):
            q.add_filter(op)
            return q

        raise ValueError(f"Unsupported type for query with input op={op}")

    def __add__(self, other: QueryFilter | QueryTransformer | Transformer) -> Query:
        """Returns a copy of the query!"""
        return self.add(other)

    def __iadd__(self, other: QueryFilter | QueryTransformer | Transformer) -> Query:
        self.append(other)
        return self

    def __getitem__(self, key: str | QueryFeature) -> QueryFeature:

        if isinstance(key, QueryFeature):
            key = key.name

        return self.stages[-1][key]

    def __eq__(self, other: Query) -> bool:
        if not isinstance(other, Query):
            return False

        return self.stages == other.stages

    def __hash__(self) -> int:
        return hash(self.stages)
