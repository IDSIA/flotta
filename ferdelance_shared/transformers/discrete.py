from typing import Any

from .core import Transformer
from ..artifacts import QueryFeature

from sklearn.preprocessing import (
    KBinsDiscretizer,
    Binarizer,
    LabelBinarizer,
    OneHotEncoder,
)

# TODO: test these classes


class FederatedKBinsDiscretizer(Transformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.KBinsDiscretizer.html#sklearn.preprocessing.KBinsDiscretizer"""

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

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedBinarizer(Transformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Binarizer.html#sklearn.preprocessing.Binarizer"""

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], threshold: float = 0) -> None:
        super().__init__(FederatedBinarizer.__name__, features_in, features_out)

        self.transformer: Binarizer = Binarizer(threshold=threshold)

        self.threshold: float = threshold

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'threshold': self.threshold,
        }

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedLabelBinarizer(Transformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelBinarizer.html"""

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

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedOneHotEncoder(Transformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder"""

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], categories: str | list[str] = 'auto', drop=None, sparse: bool = True, handle_unknown: str = 'error', min_frequency=None, max_categories=None) -> None:
        super().__init__(FederatedOneHotEncoder.__name__, features_in, features_out)

        self.transformer: OneHotEncoder = OneHotEncoder(
            categories=categories,
            drop=drop,
            sparse=sparse,
            handle_unknown=handle_unknown,
            min_frequency=min_frequency,
            max_categories=max_categories
        )

        self.categories: str | list[str] = categories
        self.drop = drop
        self.sparse: bool = sparse
        self.handle_unknown: str = handle_unknown
        self.min_frequency = min_frequency
        self.max_categories = max_categories

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'categories': self.categories,
            'drop': self.drop,
            'sparse': self.sparse,
            'handle_unknown': self.handle_unknown,
            'min_frequency': self.min_frequency,
            'max_categories': self.max_categories,
        }

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
