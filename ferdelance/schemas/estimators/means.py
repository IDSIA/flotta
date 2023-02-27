from ferdelance.schemas.estimators.core import Estimator
from ferdelance.schemas.queries import QueryFeature

import pandas as pd


class MeanEstimator(Estimator):
    def __init__(self, features_in: QueryFeature | None = None) -> None:
        super().__init__(MeanEstimator.__name__, features_in)

    def estimate(self, df: pd.DataFrame) -> float:
        # should be a single column
        vals = df[self._columns_in].mean(axis=1).values[0]
        if vals is None:
            return float("NaN")
        return vals
