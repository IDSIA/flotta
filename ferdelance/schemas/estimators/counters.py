from __future__ import annotations

from ferdelance.schemas.estimators.core import Estimator

import pandas as pd


class CountEstimator(Estimator):
    def __init__(self) -> None:
        super().__init__(CountEstimator.__name__, None)

    def estimate(self, df: pd.DataFrame) -> float:
        return df.shape[0]

    def aggregate(self, estimator_a: Estimator, estimator_b: Estimator) -> CountEstimator:
        raise NotImplementedError()
