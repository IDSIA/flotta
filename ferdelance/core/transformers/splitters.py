from typing import Any

from ferdelance.core.environment import Environment
from ferdelance.core.transformers.core import QueryTransformer

from sklearn.model_selection import train_test_split

import pandas as pd


class FederatedSplitter(QueryTransformer):
    test_percentage: float = 0.0
    stratified: bool = True

    label: str

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        df: pd.DataFrame = env["df"]

        test_p = self.test_percentage

        y = df[self.label]
        X = df.drop(self.label, axis=1)

        if test_p > 0.0:
            if self.stratified:
                env.X_tr, env.y_tr, env.X_ts, env.y_ts = train_test_split(
                    X,
                    y,
                    test_size=test_p,
                    stratify=df[self.label],
                    random_state=self.random_state,
                )

            else:
                env.X_tr, env.y_tr, env.X_ts, env.y_ts = train_test_split(
                    X,
                    y,
                    test_size=test_p,
                    random_state=self.random_state,
                )

        return env, None
