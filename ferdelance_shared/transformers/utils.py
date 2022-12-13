from .core import Transformer
from ..artifacts import QueryFeature

import pandas as pd

# TODO: test this class


class FederatedDrop(Transformer):
    """Drop a features by deleting the column(s) in the input data."""

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str]) -> None:
        """
        :param features_in:
            List of feature to be dropped by this transformer. Only a feature that exists
            in the underling DataFrame will be dropped, otherwise it will be ignored:
        """
        super().__init__(FederatedDrop.__name__, features_in, [], False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        columns_to_drop = [c for c in self.features_in if c in df.columns]
        df.drop(columns_to_drop, axis=1, inplace=True)
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()


class FederatedRename(Transformer):
    """Renames the input feature to the output features."""

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | str | list[str] | None = None, features_out: QueryFeature | list[QueryFeature] | str | list[str] | None = None) -> None:
        """
        :param features_in:
            Feature name or list of feature names that will be used as input of this transformer.
        :param features_out:
            Name of the output features. This can be a single name or a list of features. The list 
            of features as input will be renamed to these names. This is a one-to-one mapping: the
            length of `features_in` and `features_out` must be the same.
        """
        super().__init__(FederatedRename.__name__, features_in, features_out)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df.rename({f_in: f_out for f_in, f_out in zip(self.features_in, self.features_out)}, axis=1, inplace=True)
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
