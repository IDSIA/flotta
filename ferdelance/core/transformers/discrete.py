from typing import Any

from ferdelance.schemas.transformers.core import Transformer
from ferdelance.core.queries import QueryFeature

from sklearn.preprocessing import (
    KBinsDiscretizer,
    Binarizer,
    LabelBinarizer,
    OneHotEncoder,
)

import pandas as pd

# TODO: test these classes


class FederatedKBinsDiscretizer(Transformer):
    """Wrapper of scikit-learn KBinsDiscretizer. The difference is that this version
    forces the ordinal encoding of the categories and works on a single features.
    For one-hot-encoding check the FederatedOneHotEncoder transformer.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.KBinsDiscretizer.html#sklearn.preprocessing.KBinsDiscretizer
    """

    def __init__(
        self,
        features_in: QueryFeature,
        features_out: QueryFeature | str,
        n_bins: int = 5,
        strategy="uniform",
        random_state=None,
    ) -> None:
        super().__init__(FederatedKBinsDiscretizer.__name__, features_in, features_out)

        # encode is fixed to ordinal because there is a One-hot-encoder transformer
        self.transformer: KBinsDiscretizer = KBinsDiscretizer(
            n_bins=n_bins, encode="ordinal", strategy=strategy, random_state=random_state
        )

        self.n_bins: int = n_bins
        self.strategy: str = strategy
        self.random_state = random_state

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "n_bins": self.n_bins,
            "strategy": self.strategy,
            "random_state": self.random_state,
        }

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedBinarizer(Transformer):
    """Wrapper of scikit-learn Binarizer. The difference is that this version forces
    to work with a single features.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Binarizer.html#sklearn.preprocessing.Binarizer
    """

    def __init__(self, features_in: QueryFeature, features_out: QueryFeature | str, threshold: float = 0) -> None:
        """
        :param threshold:
            If the threshold is zero, the mean value will be used.
        """
        super().__init__(FederatedBinarizer.__name__, features_in, features_out)

        self.transformer: Binarizer = Binarizer(threshold=threshold)

        self.threshold: float = threshold

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "threshold": self.threshold,
        }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.fitted:
            if self.threshold == 0:
                self.threshold = df[self._columns_in].mean()[0]
                self.transformer: Binarizer = Binarizer(threshold=self.threshold)

            self.transformer.fit(df[self._columns_in])
            self.fitted = True

        df[self._columns_out] = self.transformer.transform(df[self._columns_in])
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedLabelBinarizer(Transformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelBinarizer.html"""

    def __init__(
        self,
        features_in: QueryFeature | list[QueryFeature],
        features_out: QueryFeature | list[QueryFeature] | str | list[str],
        neg_label: int = 0,
        pos_label: int = 1,
    ) -> None:
        super().__init__(FederatedLabelBinarizer.__name__, features_in, features_out)

        self.transformer: LabelBinarizer = LabelBinarizer(neg_label=neg_label, pos_label=pos_label)

        self.neg_label: int = neg_label
        self.pos_label: int = pos_label

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "neg_label": self.neg_label,
            "pos_label": self.pos_label,
        }

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedOneHotEncoder(Transformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder"""

    def __init__(
        self,
        features_in: QueryFeature,
        features_out: QueryFeature | list[QueryFeature] | str | list[str] = [],
        categories: str | list = "auto",
        drop=None,
        handle_unknown: str = "error",
        min_frequency=None,
        max_categories=None,
    ) -> None:
        super().__init__(FederatedOneHotEncoder.__name__, features_in, features_out, False)

        self.transformer: OneHotEncoder = OneHotEncoder(
            categories=categories,
            drop=drop,
            sparse=False,
            handle_unknown=handle_unknown,
            min_frequency=min_frequency,
            max_categories=max_categories,
        )

        self.categories: str | list[str] = categories
        self.drop = drop
        self.handle_unknown: str = handle_unknown
        self.min_frequency = min_frequency
        self.max_categories = max_categories

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "categories": self.categories,
            "drop": self.drop,
            "handle_unknown": self.handle_unknown,
            "min_frequency": self.min_frequency,
            "max_categories": self.max_categories,
        }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.fitted:
            self.transformer.fit(df[self._columns_in])

            cats_found = self.transformer.categories_[0]

            if self.categories == "auto":
                self._columns_out = [f"{self._columns_in[0]}_{c}" for c in range(len(cats_found))]
            elif len(self.categories) < len(cats_found):
                self._columns_out += [
                    f"{self._columns_in[0]}_{c}" for c in range(len(self.categories), len(cats_found))
                ]

            self.fitted = True

        df[self._columns_out] = self.transformer.transform(df[self._columns_in])
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
