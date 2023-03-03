from __future__ import annotations

from ferdelance.schemas.estimators.core import GenericEstimator
from ferdelance.schemas.queries.features import QueryFeature

import pandas as pd


class GroupEstimator(GenericEstimator):
    def __init__(self, feature_in: QueryFeature) -> None:
        super().__init__(GroupEstimator.__name__, feature_in)

        self.df: pd.DataFrame

    def estimate(self) -> pd.DataFrame:
        return self.df


class GroupCountEstimator(GroupEstimator):
    def __init__(self, feature_in: QueryFeature) -> None:
        super().__init__(feature_in)

    def fit(self, df: pd.DataFrame) -> None:
        self.df = df.groupby(self._columns_in, axis=1).count()

    def aggregate(self, estimator_a: GroupCountEstimator, estimator_b: GroupCountEstimator) -> GroupCountEstimator:
        if not isinstance(estimator_a, GroupCountEstimator):
            raise ValueError("Estimator left is not a GroupCountEstimator type")
        if not isinstance(estimator_b, GroupCountEstimator):
            raise ValueError("Estimator right is not a GroupCountEstimator type")
        if not estimator_a.features_in == estimator_b.features_in:
            raise ValueError("Two GroupCountEstimator have different columns")

        e = GroupCountEstimator(estimator_a.features_in[0])
        e.df = estimator_a.df.add(estimator_b.df, fill_value=0)
        return e


class GroupMeanEstimator(GroupEstimator):
    def __init__(self, feature_in: QueryFeature) -> None:
        super().__init__(feature_in)

    def fit(self, df: pd.DataFrame) -> None:
        self.df = df.groupby(self._columns_in, axis=1).mean()

    def aggregate(self, estimator_a: GroupMeanEstimator, estimator_b: GroupMeanEstimator) -> GroupMeanEstimator:
        if not isinstance(estimator_a, GroupMeanEstimator):
            raise ValueError("Estimator left is not a GroupMeanEstimator type")
        if not isinstance(estimator_b, GroupMeanEstimator):
            raise ValueError("Estimator right is not a GroupMeanEstimator type")
        if not estimator_a.features_in == estimator_b.features_in:
            raise ValueError("Two GroupMeanEstimator have different columns")

        e = GroupMeanEstimator(estimator_a.features_in[0])
        e.df = estimator_a.df.add(estimator_b.df, fill_value=0) / 2
        return e
