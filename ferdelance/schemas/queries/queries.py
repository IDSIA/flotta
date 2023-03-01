from __future__ import annotations

from ferdelance.schemas.queries.features import QueryFeature, QueryFilter, Operations
from ferdelance.schemas.queries.stages import QueryStage, QueryTransformer
from ferdelance.schemas.estimators import (
    Estimator,
    CountEstimator,
    MeanEstimator,
    GroupCountEstimator,
    GroupMeanEstimator,
)
from ferdelance.schemas.transformers import Transformer, FederatedFilter
from ferdelance.schemas.models import Model, GenericModel
from ferdelance.schemas.plans import Plan, GenericPlan

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

    def groupby(self, feature: str | QueryFeature) -> GroupingQuery:
        if isinstance(feature, str):
            feature = self[feature]

        return GroupingQuery(feature, self)

    def count(self) -> QueryEstimate:
        return self.add_estimator(CountEstimator().build())

    def mean(self, feature: str | QueryFeature) -> QueryEstimate:
        if isinstance(feature, str):
            feature = self[feature]

        return self.add_estimator(MeanEstimator().build())

    def add_estimator(self, estimator: Estimator) -> QueryEstimate:
        return QueryEstimate(
            transform=self,
            estimator=estimator,
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

    def add_plan(self, plan: GenericPlan) -> QueryPlan:
        return QueryPlan(
            transform=self,
            plan=plan.build(),
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

    def append(self, arg: QueryFilter | QueryTransformer | Transformer) -> None:
        """Add a new operation creating a new stage. This method append the new
        stage to the list of stage of the current object, without creating a new
        one.

        Args:
            arg (QueryFilter | QueryTransformer | Transformer):
                Content of the new stage to append.

        Raises:
            ValueError:
                If the argument arg is not an instance of the supported class.
        """
        if isinstance(arg, Transformer):
            arg = arg.build()

        if isinstance(arg, QueryTransformer):
            self.add_transformer(arg)
            return

        if isinstance(arg, QueryFilter):
            self.add_filter(arg)
            return

        raise ValueError(f"Unsupported type for query with input arg={arg}")

    def add(self, arg: QueryFilter | QueryTransformer | Transformer) -> Query:
        """Add a new operation creating a new stages. The return is a _new_ Query
        object with the new stage.

        Args:
            arg (QueryFilter | QueryTransformer | Transformer):
                Content of the new stage to add.

        Raises:
            ValueError:
                If the argument arg is not an instance of the supported class.

        Returns:
            Query:
                A copy of the original Query object with the new state.
        """
        if isinstance(arg, Transformer):
            arg = arg.build()

        q = self.copy(deep=True)

        if isinstance(arg, QueryTransformer):
            q.add_transformer(arg)
            return q

        if isinstance(arg, QueryFilter):
            q.add_filter(arg)
            return q

        raise ValueError(f"Unsupported type for query with input arg={arg}")

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


class GroupingQuery:
    def __init__(self, feature: QueryFeature, q: Query) -> None:
        self.q = q
        self.feature = feature

    def count(self) -> QueryEstimate:
        return self.q.add_estimator(
            GroupCountEstimator(
                feature_in=self.feature,
            ).build()
        )

    def mean(self) -> QueryEstimate:
        return self.q.add_estimator(
            GroupMeanEstimator(
                feature_in=self.feature,
            ).build()
        )


class QueryPlan(BaseModel):
    """A query with an attached plan."""

    transform: Query
    plan: Plan

    def add_model(self, model: GenericModel) -> QueryModel:
        return QueryModel(
            transform=self.transform,
            plan=self.plan,
            model=model.build(),
        )

    def add(self, arg: GenericModel) -> QueryModel:
        if isinstance(arg, GenericModel):
            return self.add_model(arg)

        raise ValueError(f"Unsupported type for query with input arg={arg}")


class QueryModel(BaseModel):
    """A query with an attached plan and model."""

    transform: Query
    plan: Plan
    model: Model


class QueryEstimate(BaseModel):
    """A query with an attached estimator."""

    transform: Query
    estimator: Estimator
