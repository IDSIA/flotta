from __future__ import annotations

from ferdelance.schemas.estimators.core import GenericEstimator

import pandas as pd


class CountEstimator(GenericEstimator):
    def __init__(self) -> None:
        super().__init__(CountEstimator.__name__, None)

        self.count: int = 0

    def fit(self, df: pd.DataFrame) -> None:
        self.count = df.shape[0]

    def estimate(self) -> float:
        return self.count

    def aggregate(self, estimator_a: CountEstimator, estimator_b: CountEstimator) -> CountEstimator:
        if not isinstance(estimator_a, CountEstimator):
            raise ValueError("Estimator left is not a CountEstimator type")
        if not isinstance(estimator_b, CountEstimator):
            raise ValueError("Estimator right is not a CountEstimator type")

        c = CountEstimator()
        c.count = estimator_a.count + estimator_b.count
        return c
