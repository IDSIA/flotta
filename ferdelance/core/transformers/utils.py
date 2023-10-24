from ferdelance.schemas.transformers.core import Transformer
from ferdelance.core.queries import QueryFeature

import pandas as pd


class FederatedDrop(Transformer):
    """Drop a features by deleting the column(s) in the input data."""

    def __init__(self, features_in: QueryFeature | list[QueryFeature] | list[str] | str) -> None:
        """
        :param features_in:
            List of feature to be dropped by this transformer. Only a feature that exists
            in the underling DataFrame will be dropped, otherwise it will be ignored:
        """
        if isinstance(features_in, str):
            features_in = [QueryFeature(name=features_in, dtype=None)]
        if isinstance(features_in, list):
            li = list()
            for f in features_in:
                if isinstance(f, QueryFeature):
                    li.append(f)
                else:
                    li.append(QueryFeature(name=f, dtype=None))
            features_in = li

        super().__init__(FederatedDrop.__name__, features_in, [], False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        columns_to_drop = [c for c in self._columns_in if c in df.columns]
        df.drop(columns_to_drop, axis=1, inplace=True)
        return df

    def aggregate(self) -> None:
        # TODO: do we need this?
        return super().aggregate()


def convert_list(features: str | list[str] | QueryFeature | list[QueryFeature]) -> list[QueryFeature]:
    if isinstance(features, str):
        return [QueryFeature(name=features, dtype=None)]

    if isinstance(features, QueryFeature):
        return [features]

    if isinstance(features, list):
        ret: list[QueryFeature] = list()

        for f in features:
            if isinstance(f, QueryFeature):
                ret.append(f)
            else:
                ret.append(QueryFeature(name=f, dtype=None))

        return ret


class FederatedRename(Transformer):
    """Renames the input feature to the output features."""

    def __init__(
        self,
        features_in: QueryFeature | list[QueryFeature] | str | list[str],
        features_out: QueryFeature | list[QueryFeature] | str | list[str],
    ) -> None:
        """
        :param features_in:
            Feature name or list of feature names that will be used as input of this transformer.
        :param features_out:
            Name of the output features. This can be a single name or a list of features. The list
            of features as input will be renamed to these names. This is a one-to-one mapping: the
            length of `features_in` and `features_out` must be the same.
        """
        super().__init__(FederatedRename.__name__, convert_list(features_in), convert_list(features_out))

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df.rename({f_in: f_out for f_in, f_out in zip(self._columns_in, self._columns_out)}, axis=1, inplace=True)
        return df

    def aggregate(self) -> None:
        # TODO: do we need this?
        return super().aggregate()
