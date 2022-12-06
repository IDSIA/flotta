from typing import Any

from .core import Transformer
from ..artifacts import QueryFeature

from sklearn.impute import SimpleImputer

import numpy as np

# TODO: test these classes


class FederatedSimpleImputer(Transformer):

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], missing_values: float = np.nan, strategy: str = "mean", fill_value=None, add_indicator: bool = False) -> None:
        super().__init__(FederatedSimpleImputer.__name__, features_in, features_out)

        self.transformer: SimpleImputer = SimpleImputer(
            missing_values=missing_values,
            strategy=strategy,
            fill_value=fill_value,
            add_indicator=add_indicator,
        )
        self.missing_values = missing_values
        self.strategy = strategy
        self.fill_value = fill_value
        self.add_indicator = add_indicator

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'missing_values': self.missing_values,
            'strategy': self.strategy,
            'fill_value': self.fill_value,
            'add_indicator': self.add_indicator,
        }

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
