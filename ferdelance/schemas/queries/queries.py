from __future__ import annotations

from ferdelance.schemas.queries.features import QueryFeature, QueryFilter, Operations
from ferdelance.schemas.queries.stages import QueryStage, QueryTransformer
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
    """A query is just a list of stages applied to the input data."""

    stages: list[QueryStage] = list()

    def copy(self) -> Query:
        # this is a shallow copy!
        return Query(stages=self.stages.copy())

    def current(self) -> QueryStage:
        return self.stages[-1]

    def features(self) -> list[QueryFeature]:
        s: QueryStage = self.current()
        return s.features

    def feature(self, key: str | QueryFeature) -> QueryFeature:
        if isinstance(key, QueryFeature):
            key = key.name

        return self.current()[key]

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
                    op=Operations[filter.operation],
                    value=filter.value,
                ).build(),
            )
        )

    def add(self, op: QueryFilter | QueryTransformer | Transformer) -> None:
        if isinstance(op, Transformer):
            op = op.build()

        if isinstance(op, QueryTransformer):
            self.add_transformer(op)
            return

        if isinstance(op, QueryFilter):
            self.add_filter(op)
            return

        raise ValueError(f"Unsupported type for query with input op={op}")

    def __add__(self, other: QueryFilter | QueryTransformer | Transformer) -> Query:
        """Returns a copy of the query!"""
        q = self.copy()
        q.add(other)
        return q

    def __iadd__(self, other: QueryFilter | QueryTransformer | Transformer) -> Query:
        self.add(other)
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
