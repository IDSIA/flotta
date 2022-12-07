from .core import Transformer
from ..artifacts import QueryFeature

import pandas as pd

# TODO: test this class


class FederatedDrop(Transformer):
    """Drop a features by deleting the column(s) in the input data."""

    def __init__(self, feature_in: QueryFeature | list[QueryFeature] | str | list[str]) -> None:
        super().__init__(FederatedDrop.__name__, feature_in, [], False)

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
        super().__init__(FederatedRename.__name__, features_in, features_out)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df.rename({f_in: f_out for f_in, f_out in zip(self.features_in, self.features_out)}, axis=1, inplace=True)
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
