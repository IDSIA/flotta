from typing import Any

from ferdelance.schemas.queries import QueryFeature, Query
from ferdelance.schemas.estimators.core import Estimator

import pandas as pd


class GroupEstimator(Estimator):
    def __init__(self, feature_in: QueryFeature, agg: str) -> None:
        super().__init__(GroupEstimator.__name__, feature_in)
        self.agg = agg

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "agg": self.agg,
        }

    def estimate(self, df: pd.DataFrame) -> float:
        return super().estimate(df)


class GroupingQuery:
    def __init__(self, feature: QueryFeature, q: Query) -> None:
        self.q = q
        self.feature = feature

    def count(self) -> Query:
        self.q.add_estimator(
            GroupEstimator(
                feature_in=self.feature,
                agg="count",
            ).build()
        )
        return self.q

    def mean(self) -> Query:
        self.q.add_estimator(
            GroupEstimator(
                feature_in=self.feature,
                agg="mean",
            ).build()
        )
        return self.q
