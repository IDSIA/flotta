from typing import Any

from .core import Transformer
from ..artifacts import QueryFeature

from sklearn.preprocessing import (
    MinMaxScaler,
    StandardScaler,
)

import pandas as pd


class FederatedMinMaxScaler(Transformer):

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], feature_range: tuple = (0, 1)) -> None:
        super().__init__(FederatedMinMaxScaler.__name__, features_in, features_out)

        self.transformer: MinMaxScaler = MinMaxScaler(feature_range=feature_range)

        self.feature_range: tuple[float, float] = feature_range

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'feature_range': self.feature_range,
        }

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedStandardScaler(Transformer):

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], with_mean: bool = True, with_std: bool = True) -> None:
        super().__init__(FederatedStandardScaler.__name__, features_in, features_out)

        self.scaler: StandardScaler = StandardScaler(with_mean=with_mean, with_std=with_std)
        self.with_mean: bool = with_mean
        self.with_std: bool = with_std

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'with_mean': self.with_mean,
            'with_std': self.with_std,
        }

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
