from __future__ import annotations

from ferdelance.schemas.estimators.core import GenericEstimator
from ferdelance.schemas.queries import QueryFeature

import pandas as pd


class MeanEstimator(GenericEstimator):
    def __init__(self, feature_in: QueryFeature | None = None) -> None:
        super().__init__(MeanEstimator.__name__, feature_in)

        self.mean: float = 0

    def fit(self, df: pd.DataFrame) -> None:
        # should be a single column
        vals = df[self._columns_in].mean(axis=1).values[0]
        if vals is None:
            self.mean = float("NaN")
        else:
            self.mean = vals

    def estimate(self) -> float:
        return self.mean

    def aggregate(self, estimator_a: MeanEstimator, estimator_b: MeanEstimator) -> MeanEstimator:
        if not isinstance(estimator_a, MeanEstimator):
            raise ValueError("Estimator left is not a MeanEstimator type")
        if not isinstance(estimator_b, MeanEstimator):
            raise ValueError("Estimator right is not a MeanEstimator type")

        c = MeanEstimator()
        c.mean = (estimator_a.mean + estimator_b.mean) / 2
        return c
