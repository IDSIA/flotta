from typing import Any

from .core import Transformer
from ..artifacts import (
    QueryFeature,
)

from sklearn.preprocessing import MinMaxScaler

import pandas as pd


class FederatedMinMaxScaler(Transformer):

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], feature_range: tuple = (0, 1)) -> None:
        super().__init__(FederatedMinMaxScaler.__name__, features_in, features_out)

        self.scaler: MinMaxScaler = MinMaxScaler(feature_range=feature_range)
        self.feature_range: tuple[float, float] = feature_range

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'feature_range': self.feature_range,
        }

    def dict(self) -> dict[str, Any]:
        return super().dict() | {
            'scaler': self.scaler,
        }

    def fit(self, df: pd.DataFrame) -> None:
        self.scaler.fit(df[self.features_in])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.features_out] = self.scaler.transform(df[self.features_in])
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
