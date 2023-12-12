from typing import Any

from ferdelance.core.environment import Environment
from ferdelance.core.transformers.core import QueryTransformer
from ferdelance.core.queries import QueryFeature


class FederatedDrop(QueryTransformer):
    """Drop a features by deleting the column(s) in the input data."""

    def __init__(self, features_in: list[QueryFeature]) -> None:
        """
        :param features_in:
            List of feature to be dropped by this transformer. Only a feature that exists
            in the underling DataFrame will be dropped, otherwise it will be ignored:
        """
        if isinstance(features_in, str):
            features_in = [QueryFeature(features_in)]
        if isinstance(features_in, list):
            li = list()
            for f in features_in:
                if isinstance(f, QueryFeature):
                    li.append(f)
                else:
                    li.append(QueryFeature(f))
            features_in = li

        super().__init__(features_in=features_in, features_out=[])

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        c_in = self._columns_in()

        if env.X_tr is not None:
            columns_to_drop = [c for c in c_in if c in env.X_tr.columns]
            env.X_tr = env.X_tr.drop(columns_to_drop, axis=1, inplace=False)

        if env.X_ts is not None:
            columns_to_drop = [c for c in c_in if c in env.X_ts.columns]
            env.X_ts = env.X_ts.drop(columns_to_drop, axis=1, inplace=False)

        return env, None

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        return super().aggregate(env)


class FederatedRename(QueryTransformer):
    """Renames the input feature to the output features."""

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        c_in = self._columns_in()
        c_out = self._columns_out()

        rename_dict = {f_in: f_out for f_in, f_out in zip(c_in, c_out)}

        if env.X_tr is not None:
            env.X_tr = env.X_tr.rename(rename_dict, axis=1, inplace=False)

        if env.Y_tr is not None:
            env.Y_tr = env.Y_tr.rename(rename_dict, axis=1, inplace=False)

        if env.X_ts is not None:
            env.X_ts = env.X_ts.rename(rename_dict, axis=1, inplace=False)

        if env.Y_ts is not None:
            env.Y_ts = env.Y_ts.rename(rename_dict, axis=1, inplace=False)

        return env, None

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        return super().aggregate(env)
