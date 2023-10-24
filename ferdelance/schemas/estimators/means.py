from __future__ import annotations
from typing import Any

from ferdelance.schemas.estimators.core import GenericEstimator
from ferdelance.core.queries import QueryFeature

import numpy as np
import pandas as pd


class MeanEstimator(GenericEstimator):
    def __init__(self, feature_in: QueryFeature | None = None, random_state: Any = None) -> None:
        super().__init__(MeanEstimator.__name__, feature_in, random_state)

        shape = (3, len(self._columns_in))

        zeros = np.zeros(shape)

        self.sum = zeros[0]
        self.count = zeros[1]
        self.mean = zeros[2]

    def initialize(self) -> None:
        r = np.random.default_rng(self.random_state)

        shape = (2, len(self._columns_in))
        vals = r.integers(-(2**31), 2**31, size=shape)

        self.sum = vals[0]
        self.count = vals[1]
        self.mean = self.sum / self.count

    def fit(self, df: pd.DataFrame) -> None:
        self.sum += df[self._columns_in].sum(axis=1)
        self.count += df.shape[0]
        self.mean = self.sum / self.count

    def aggregate(self, estimator_a: MeanEstimator, estimator_b: MeanEstimator) -> MeanEstimator:
        if not isinstance(estimator_a, MeanEstimator):
            raise ValueError("Estimator left is not a MeanEstimator type")
        if not isinstance(estimator_b, MeanEstimator):
            raise ValueError("Estimator right is not a MeanEstimator type")

        c = MeanEstimator()

        c.sum = estimator_a.sum + estimator_b.sum
        c.count = estimator_a.count + estimator_b.count
        c.mean = c.sum / c.count

        return c

    def finalize(self, estimator: GenericEstimator) -> None:
        if not isinstance(estimator, MeanEstimator):
            raise ValueError("Invalid finalization estimator")

        self.sum -= estimator.sum
        self.count -= estimator.count

        self.mean = self.sum / self.count

    def estimate(self) -> float:
        return self.mean
