from typing import Any, Literal

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

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        tr = KBinsDiscretizer(
            n_bins=self.n_bins,
            encode="ordinal",
            strategy=self.strategy,
            random_state=self.random_state,
        )

        if X_tr is None:
            raise ValueError("X_tr required!")

        if X_ts is None:
            X_ts = X_tr
        else:
            X_ts = X_ts

        tr.fit(X_tr[self._columns_in()])
        X_ts[self._columns_out()] = tr.transform(X_ts[self._columns_in()])

        return X_tr, y_tr, X_ts, y_ts, tr

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)


class FederatedBinarizer(QueryTransformer):
    """Wrapper of scikit-learn Binarizer. The difference is that this version forces
    to work with a single features.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Binarizer.html#sklearn.preprocessing.Binarizer
    """

    threshold: float = 0

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        tr = Binarizer(
            threshold=self.threshold,
        )

        if X_tr is None:
            raise ValueError("X_tr required!")

        if self.threshold == 0:
            self.threshold = X_tr[self._columns_in()].mean()[0]

        tr.fit(X_tr[self._columns_in()])

        if X_ts is None:
            X_ts = X_tr
        else:
            X_ts = X_ts

        X_ts[self._columns_out()] = tr.transform(X_ts[self._columns_in()])

        return X_tr, y_tr, X_ts, y_ts, tr

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)


class FederatedLabelBinarizer(QueryTransformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelBinarizer.html"""

    neg_label: int = 0
    pos_label: int = 1

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        tr = LabelBinarizer(
            neg_label=self.neg_label,
            pos_label=self.pos_label,
        )

        if y_tr is None:
            raise ValueError("No label column given!")

        tr.fit(y_tr)

        if y_ts is None:
            y_ts = y_tr
        else:
            y_ts = y_ts

        y_ts = tr.transform(y_ts)  # type: ignore # TODO: check this

        return X_tr, y_tr, X_ts, y_ts, tr

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)


class FederatedOneHotEncoder(QueryTransformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder"""

    categories: str | list = "auto"
    drop = None
    handle_unknown: Literal["error", "ignore", "infrequent_if_exist"] = "error"
    min_frequency = None
    max_categories = None
    sparse: bool = False

    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        tr = OneHotEncoder(
            categories=self.categories,
            drop=self.drop,
            sparse=self.sparse,
            handle_unknown=self.handle_unknown,
            min_frequency=self.min_frequency,
            max_categories=self.max_categories,
        )

        c_in = self._columns_in()
        c_out = self._columns_out()

        if X_tr is None:
            raise ValueError("X_tr required!")

        tr.fit(X_tr[self._columns_in()])

        cats_found = tr.categories_[0]
        n_cats = len(cats_found)  # type: ignore # TODO: check this

        if self.categories == "auto":
            c_out = [f"{c_in[0]}_{c}" for c in range(n_cats)]
        elif len(self.categories) < n_cats:
            c_out += [f"{c_in[0]}_{c}" for c in range(len(self.categories), n_cats)]

        if X_ts is None:
            X_ts = X_tr
        else:
            X_ts = X_ts

        X_tr[c_out] = tr.transform(X_tr[c_in])

        return X_tr, y_tr, X_ts, y_ts, tr

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)
