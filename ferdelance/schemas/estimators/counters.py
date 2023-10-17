from __future__ import annotations
from typing import Any

from ferdelance.schemas.estimators.core import GenericEstimator

import numpy as np
import pandas as pd


class CountEstimator(GenericEstimator):
    def __init__(self, random_state: Any = None) -> None:
        super().__init__(CountEstimator.__name__, None, random_state)

        self.count: int = 0

    def initialize(self) -> None:
        r = np.random.default_rng(self.random_state)
        self.count = r.integers(-(2**31), 2**31, size=1)[0]

    def fit(self, df: pd.DataFrame) -> None:
        self.count += df.shape[0]

    def aggregate(self, estimator_a: CountEstimator, estimator_b: CountEstimator) -> CountEstimator:
        if not isinstance(estimator_a, CountEstimator):
            raise ValueError("Estimator left is not a CountEstimator type")
        if not isinstance(estimator_b, CountEstimator):
            raise ValueError("Estimator right is not a CountEstimator type")

        c = CountEstimator()
        c.count = estimator_a.count + estimator_b.count
        return c

    def finalize(self, estimator: GenericEstimator) -> None:
        if not isinstance(estimator, CountEstimator):
            raise ValueError("Invalid finalization estimator")

        self.count -= estimator.count

    def estimate(self) -> float:
        return self.count
