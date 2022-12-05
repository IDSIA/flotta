from typing import Any

from .core import Transformer
from ..artifacts import QueryFeature

from sklearn.preprocessing import (
    KBinsDiscretizer,
    Binarizer,
    LabelBinarizer,
)

import pandas as pd

# TODO: test these classes


class FederatedKBinsDiscretizer(Transformer):

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], n_bins: int = 5, encode: str = 'ordinal', strategy='uniform', random_state=None) -> None:
        super().__init__(FederatedKBinsDiscretizer.__name__, features_in, features_out)

        self.transformer: KBinsDiscretizer = KBinsDiscretizer(n_bins=n_bins, encode=encode, strategy=strategy, random_state=random_state)

        self.n_bins: int = n_bins
        self.encode: str = encode
        self.strategy: str = strategy
        self.random_state = random_state

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'n_bins': self.n_bins,
            'encode': self.encode,
            'strategy': self.strategy,
            'random_state': self.random_state,
        }

    def dict(self) -> dict[str, Any]:
        return super().dict() | {
            'transformer': self.transformer,
        }

    def fit(self, df: pd.DataFrame) -> None:
        self.transformer.fit(df[self.features_in])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.features_out] = self.transformer.transform(df[self.features_in])
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedBinarizer(Transformer):

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], threhsold: float = 0) -> None:
        super().__init__(FederatedBinarizer.__name__, features_in, features_out)

        self.transformer: Binarizer = Binarizer(threshold=threhsold)

        self.threhsold: float = threhsold

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'threhsold': self.threhsold,
        }

    def dict(self) -> dict[str, Any]:
        return super().dict() | {
            'transformer': self.transformer,
        }

    def fit(self, df: pd.DataFrame) -> None:
        self.transformer.fit(df[self.features_in])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.features_out] = self.transformer.transform(df[self.features_in])
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedLabelBinarizer(Transformer):

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], neg_label: int = 0, pos_label: int = 1) -> None:
        super().__init__(FederatedLabelBinarizer.__name__, features_in, features_out)

        self.transformer: LabelBinarizer = LabelBinarizer(neg_label=neg_label, pos_label=pos_label)

        self.neg_label: int = neg_label
        self.pos_label: int = pos_label

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'neg_label': self.neg_label,
            'pos_label': self.pos_label,
        }

    def dict(self) -> dict[str, Any]:
        return super().dict() | {
            'transformer': self.transformer,
        }

    def fit(self, df: pd.DataFrame) -> None:
        self.transformer.fit(df[self.features_in])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.features_out] = self.transformer.transform(df[self.features_in])
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
