from typing import Any

from ferdelance.schemas.transformers.core import Transformer
from ferdelance.schemas.queries import QueryFeature

from sklearn.impute import SimpleImputer

import numpy as np


class FederatedSimpleImputer(Transformer):
    def __init__(
        self,
        features_in: QueryFeature | list[QueryFeature],
        features_out: QueryFeature | list[QueryFeature] | str | list[str],
        missing_values: float = np.nan,
        strategy: str = "mean",
        fill_value=None,
    ) -> None:
        super().__init__(FederatedSimpleImputer.__name__, features_in, features_out)

        if fill_value is not None:
            strategy = "constant"

        self.transformer: SimpleImputer = SimpleImputer(
            missing_values=missing_values,
            strategy=strategy,
            fill_value=fill_value,
        )
        self.missing_values = missing_values
        self.strategy = strategy
        self.fill_value = fill_value

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "missing_values": self.missing_values,
            "strategy": self.strategy,
            "fill_value": self.fill_value,
        }

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
