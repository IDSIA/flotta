from typing import Any
from ferdelance.core.transformers.core import QueryTransformer
from ferdelance.core.queries import QueryFeature

import pandas as pd


class FederatedDrop(QueryTransformer):
    """Drop a features by deleting the column(s) in the input data."""

    def __init__(self, features_in: list[QueryFeature]) -> None:
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

        super().__init__(features_in=features_in, features_out=[])

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        c_in = self._columns_in()

        if X_tr is not None:
            columns_to_drop = [c for c in c_in if c in X_tr.columns]
            X_tr = X_tr.drop(columns_to_drop, axis=1, inplace=False)

        if X_ts is not None:
            columns_to_drop = [c for c in c_in if c in X_ts.columns]
            X_ts = X_ts.drop(columns_to_drop, axis=1, inplace=False)

        return X_tr, y_tr, X_ts, y_ts, None

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)


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


class FederatedRename(QueryTransformer):
    """Renames the input feature to the output features."""

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        c_in = self._columns_in()
        c_out = self._columns_out()

        rename_dict = {f_in: f_out for f_in, f_out in zip(c_in, c_out)}

        if X_tr is not None:
            X_tr = X_tr.rename(rename_dict, axis=1, inplace=False)

        if X_ts is not None:
            X_ts = X_ts.rename(rename_dict, axis=1, inplace=False)

        return X_tr, y_tr, X_ts, y_ts, None

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)
