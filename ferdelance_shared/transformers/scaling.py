from typing import Any

from .core import Transformer
from ..artifacts import QueryFeature

from sklearn.preprocessing import (
    MinMaxScaler,
    StandardScaler,
)


class FederatedMinMaxScaler(Transformer):
    """Wrapper of scikit-learn MinMaxScaler.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MinMaxScaler.html
    """

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], feature_range: tuple = (0, 1)) -> None:
        """
        :param features_in:
            Name of the input feature or list of feature to scale down.
        :param features_out:
            Name of the output feature or list of features that have been scaled down. This
            list is a one-to-one match with the features_in parameter.
        :param feature_range:
            Range of the values in the format (min, max). Same as scikit-learn MinMaxScaler 
            `feature_range` parameter. Quote:

            "Desired range of transformed data."
        """
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
    """Wrapper of scikit-learn StandardScaler.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html
    """

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str], features_out: QueryFeature | list[QueryFeature] | str | list[str], with_mean: bool = True, with_std: bool = True) -> None:
        """
        :param features_in:
            Name of the input feature or list of feature to scale down.
        :param features_out:
            Name of the output feature or list of features that have been scaled down. This
            list is a one-to-one match with the features_in parameter.
        :param with_mean:
            Same as scikit-learn StandardScaler `with_mean` parameter. Quote:

            "If True, center the data before scaling. This does not work (and will raise an 
            exception) when attempted on sparse matrices, because centering them entails 
            building a dense matrix which in common use cases is likely to be too large 
            to fit in memory."

        :param with_std:
            Same as scikit-learn StandardScaler `with_std` parameter. Quote:

            "If True, scale the data to unit variance (or equivalently, unit standard deviation)."
        """
        super().__init__(FederatedStandardScaler.__name__, features_in, features_out)

        self.transformer: StandardScaler = StandardScaler(with_mean=with_mean, with_std=with_std)
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
