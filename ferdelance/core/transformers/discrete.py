from typing import Any, Literal

from ferdelance.core.environment import Environment
from ferdelance.core.transformers.core import QueryTransformer

from sklearn.preprocessing import (
    KBinsDiscretizer,
    Binarizer,
    LabelBinarizer,
    OneHotEncoder,
)

import pandas as pd


class FederatedKBinsDiscretizer(QueryTransformer):
    """Wrapper of scikit-learn KBinsDiscretizer. The difference is that this version
    forces the ordinal encoding of the categories and works on a single features.
    For one-hot-encoding check the FederatedOneHotEncoder transformer.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.KBinsDiscretizer.html#sklearn.preprocessing.KBinsDiscretizer
    """

    n_bins: int = 5
    strategy: Literal["uniform", "quantile", "kmeans"] = "uniform"

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        if env.X_tr is None:
            raise ValueError("X_tr required!")

        tr = KBinsDiscretizer(
            n_bins=self.n_bins,
            encode="ordinal",
            strategy=self.strategy,
            random_state=self.random_state,
        )

        tr.fit(env.X_tr[self._columns_in()])

        if env.X_ts is None:
            X = env.X_tr
        else:
            X = env.X_tr

        X[self._columns_out()] = tr.transform(X[self._columns_in()])

        return env, tr

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        raise NotImplementedError()


class FederatedBinarizer(QueryTransformer):
    """Wrapper of scikit-learn Binarizer. The difference is that this version forces
    to work with a single features.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Binarizer.html#sklearn.preprocessing.Binarizer
    """

    threshold: float = 0

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        if env.X_tr is None:
            raise ValueError("X_tr required!")

        c_in = self._columns_in()
        c_out = self._columns_out()

        if self.threshold == 0:
            self.threshold = env.X_tr[c_in].mean().iloc[0]

        tr = Binarizer(
            threshold=self.threshold,
        )
        tr.fit(env.X_tr[c_in])

        if env.X_ts is None:
            X = env.X_tr
        else:
            X = env.X_ts

        X[c_out] = tr.transform(X[c_in])

        return env, tr

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        raise NotImplementedError()


class FederatedLabelBinarizer(QueryTransformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelBinarizer.html"""

    neg_label: int = 0
    pos_label: int = 1

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        if env.X_tr is None:
            raise ValueError("No train data given!")

        tr = LabelBinarizer(
            neg_label=self.neg_label,
            pos_label=self.pos_label,
        )

        c_in = self._columns_in()
        c_out = self._columns_out()

        tr_label_cols = env.X_tr[c_in]
        tr_label_data = tr.fit_transform(tr_label_cols)

        env.Y_tr = pd.DataFrame(data=tr_label_data, columns=c_out)  # type: ignore
        env.X_tr.drop(c_in, inplace=True, axis=1)

        if env.X_ts is not None:
            ts_label_cols = env.X_ts[c_in]
            ts_label_data = tr.transform(ts_label_cols)

            env.Y_ts = pd.DataFrame(data=ts_label_data, columns=c_out)  # type: ignore
            env.X_ts.drop(c_in, inplace=True, axis=1)

        return env, tr

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        raise NotImplementedError()


class FederatedOneHotEncoder(QueryTransformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder"""

    categories: str | list = "auto"
    drop: list | None = None
    handle_unknown: Literal["error", "ignore", "infrequent_if_exist"] = "error"
    min_frequency: float | int | None = None
    max_categories: int | None = None

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        if env.X_tr is None:
            raise ValueError("X_tr required!")

        tr = OneHotEncoder(
            categories=self.categories,
            drop=self.drop,
            sparse_output=False,
            handle_unknown=self.handle_unknown,
            min_frequency=self.min_frequency,
            max_categories=self.max_categories,
        )

        c_in = self._columns_in()
        c_out = self._columns_out()

        tr.fit(env.X_tr[c_in])

        cats_found = tr.categories_[0]
        n_cats = len(cats_found)  # type: ignore # TODO: check this

        if self.categories == "auto":
            c_out = [f"{c_in[0]}_{c}" for c in range(n_cats)]
        elif len(self.categories) < n_cats:
            c_out += [f"{c_in[0]}_{c}" for c in range(len(self.categories), n_cats)]

        if env.X_ts is None:
            X = env.X_tr
        else:
            X = env.X_ts

        X[c_out] = tr.transform(X[c_in])

        return env, tr

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        raise NotImplementedError()
