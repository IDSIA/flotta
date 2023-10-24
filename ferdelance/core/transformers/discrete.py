from typing import Any, Literal

from ferdelance.core.transformers.core import Transformer

from sklearn.preprocessing import (
    KBinsDiscretizer,
    Binarizer,
    LabelBinarizer,
    OneHotEncoder,
)

# TODO: test these classes


class FederatedKBinsDiscretizer(Transformer):
    """Wrapper of scikit-learn KBinsDiscretizer. The difference is that this version
    forces the ordinal encoding of the categories and works on a single features.
    For one-hot-encoding check the FederatedOneHotEncoder transformer.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.KBinsDiscretizer.html#sklearn.preprocessing.KBinsDiscretizer
    """

    n_bins: int = 5
    strategy: Literal["uniform", "quantile", "kmeans"] = "uniform"
    random_state: Any = None

    def get_transformer(self) -> KBinsDiscretizer:
        return KBinsDiscretizer(
            n_bins=self.n_bins,
            encode="ordinal",
            strategy=self.strategy,
            random_state=self.random_state,
        )

    def fit(self, env: dict[str, Any]) -> dict[str, Any]:
        return env

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)


class FederatedBinarizer(Transformer):
    """Wrapper of scikit-learn Binarizer. The difference is that this version forces
    to work with a single features.

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Binarizer.html#sklearn.preprocessing.Binarizer
    """

    threshold: float

    def get_transformer(self) -> Any:
        return Binarizer(
            threshold=self.threshold,
        )

    def transform(self, env: dict[str, Any]) -> dict[str, Any]:
        df = env["df"]
        transformer = env.get("transformer", self.get_transformer())

        if not self.fitted:
            if self.threshold == 0:
                self.threshold = df[self._columns_in].mean()[0]
                transformer: Binarizer = self.get_transformer()

            transformer.fit(df[self._columns_in])
            self.fitted = True

        df[self._columns_out] = transformer.transform(df[self._columns_in])

        env["df"] = df

        return env

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)


class FederatedLabelBinarizer(Transformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelBinarizer.html"""

    neg_label: int = 0
    pos_label: int = 1

    def get_transformer(self) -> Any:
        return LabelBinarizer(
            neg_label=self.neg_label,
            pos_label=self.pos_label,
        )

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)


class FederatedOneHotEncoder(Transformer):
    """Reference: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder"""

    categories: str | list = "auto"
    drop = None
    handle_unknown: Literal["error", "ignore", "infrequent_if_exist"] = "error"
    min_frequency = None
    max_categories = None
    sparse: bool = False

    def get_transformer(self) -> Any:
        return OneHotEncoder(
            categories=self.categories,
            drop=self.drop,
            sparse=self.sparse,
            handle_unknown=self.handle_unknown,
            min_frequency=self.min_frequency,
            max_categories=self.max_categories,
        )

    def transform(self, env: dict[str, Any]) -> dict[str, Any]:
        df = env["df"]
        transformer = env.get("transformer", self.get_transformer())

        if not self.fitted:
            transformer.fit(df[self._columns_in])

            cats_found = transformer.categories_[0]

            if self.categories == "auto":
                self._columns_out = [f"{self._columns_in[0]}_{c}" for c in range(len(cats_found))]
            elif len(self.categories) < len(cats_found):
                self._columns_out += [
                    f"{self._columns_in[0]}_{c}" for c in range(len(self.categories), len(cats_found))
                ]

            self.fitted = True

        df[self._columns_out] = transformer.transform(df[self._columns_in])

        env["df"] = df

        return env

    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        # TODO
        return super().aggregate(env)
