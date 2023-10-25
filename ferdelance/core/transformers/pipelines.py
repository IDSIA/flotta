from typing import Any, Sequence

from ferdelance.core.transformers.core import QueryTransformer

import pandas as pd


class FederatedPipeline(QueryTransformer):
    """A pipeline that can be used to group sequence of Transformers.
    The stages of the pipeline will be applied in sequence to the input data.

    A pipeline can also be nested inside another pipeline.
    """

    stages: Sequence[QueryTransformer] = list()

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        for stage in self.stages:
            X_tr, y_tr, X_ts, y_ts, _ = stage.transform(X_tr, y_tr, X_ts, y_ts)

        return X_tr, y_tr, X_ts, y_ts, None

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)
