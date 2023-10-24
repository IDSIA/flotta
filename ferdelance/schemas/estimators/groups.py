from __future__ import annotations

from ferdelance.schemas.estimators.core import GenericEstimator
from ferdelance.core.queries.features import QueryFeature

import pandas as pd
import numpy as np


class GroupEstimator(GenericEstimator):
    def __init__(self, feature_in: QueryFeature) -> None:
        super().__init__(GroupEstimator.__name__, feature_in)

        self.df: pd.DataFrame

    def estimate(self) -> pd.DataFrame:
        return self.df


class GroupCountEstimator(GroupEstimator):
    def __init__(self, feature_in: QueryFeature) -> None:
        super().__init__(feature_in)

    def initialize(self) -> None:
        r = np.random.default_rng(self.random_state)

        shape = (1, len(self._columns_in))
        vals = r.integers(-(2**31), 2**31, size=shape)

        self.df = pd.DataFrame(
            data=vals,
            columns=self._columns_in,
        )

    def fit(self, df: pd.DataFrame) -> None:
        self.df.add(df.groupby(self._columns_in, axis=1).count())

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

    def finalize(self, estimator: GenericEstimator) -> None:
        if not isinstance(estimator, GroupCountEstimator):
            raise ValueError("Invalid finalization estimator")

        self.df.subtract(estimator.df)


class GroupMeanEstimator(GroupEstimator):
    def __init__(self, feature_in: QueryFeature) -> None:
        super().__init__(feature_in)

        self.df_sum: pd.DataFrame
        self.df_count: pd.DataFrame

    def initialize(self) -> None:
        r = np.random.default_rng(self.random_state)

        shape = (2, len(self._columns_in))
        vals = r.integers(-(2**31), 2**31, size=shape)

        self.sum = pd.DataFrame(data=vals[0], columns=self._columns_in)
        self.count = pd.DataFrame(data=vals[1], columns=self._columns_in)

        self.df = self.sum.divide(self.count)

    def fit(self, df: pd.DataFrame) -> None:
        df_sum = df.groupby(self._columns_in, axis=1).sum()
        df_count = df.groupby(self._columns_in, axis=1).count()

        self.sum.add(df_sum)
        self.count.add(df_count)

        self.df = self.sum.divide(self.count)

    def aggregate(self, estimator_a: GroupMeanEstimator, estimator_b: GroupMeanEstimator) -> GroupMeanEstimator:
        if not isinstance(estimator_a, GroupMeanEstimator):
            raise ValueError("Estimator left is not a GroupMeanEstimator type")
        if not isinstance(estimator_b, GroupMeanEstimator):
            raise ValueError("Estimator right is not a GroupMeanEstimator type")
        if not estimator_a.features_in == estimator_b.features_in:
            raise ValueError("Two GroupMeanEstimator have different columns")

        e = GroupMeanEstimator(estimator_a.features_in[0])

        e.df_sum = estimator_a.df_sum.add(estimator_b.df_sum)
        e.df_count = estimator_a.df_count.add(estimator_b.df_count)

        e.df = e.sum.divide(e.count)

        return e

    def finalize(self, estimator: GenericEstimator) -> None:
        if not isinstance(estimator, GroupMeanEstimator):
            raise ValueError("Invalid finalization estimator")

        self.df_sum.subtract(estimator.df_sum)
        self.df_count.subtract(estimator.df_count)

        self.df = self.sum.divide(self.count)
