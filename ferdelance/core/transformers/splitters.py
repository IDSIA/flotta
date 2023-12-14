from typing import Any

from ferdelance.core.environment import Environment
from ferdelance.core.transformers.core import QueryTransformer

from sklearn.model_selection import train_test_split


class FederatedSplitter(QueryTransformer):
    test_percentage: float = 0.0
    stratified: bool = True

    label: str

    def aggregate(self, env: Environment) -> Environment:
        return env

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        if env.df is None:
            raise ValueError("Trying to apply FederatedSplitter to an environment without DataFrame df")

        test_p = self.test_percentage

        Y = env.df[self.label]
        X = env.df.drop(self.label, axis=1)

        if test_p > 0.0:
            if self.stratified:
                env.X_tr, env.X_ts, env.Y_tr, env.Y_ts = train_test_split(
                    X,
                    Y,
                    test_size=test_p,
                    stratify=env.df[self.label],
                    random_state=self.random_state,
                )

            else:
                env.X_tr, env.X_ts, env.Y_tr, env.Y_ts = train_test_split(
                    X,
                    Y,
                    test_size=test_p,
                    random_state=self.random_state,
                )

        return env, None
