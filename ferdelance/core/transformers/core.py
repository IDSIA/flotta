from __future__ import annotations
from typing import Any
from abc import ABC, abstractmethod

from ferdelance.core.entity import Entity
from ferdelance.core.queries import QueryFeature

import pandas as pd


class QueryTransformer(ABC, Entity):
    """Basic class that defines a transformer. A transformer is an object that can transform
    input data. This transformation is used as a pre-processing that need to be applied
    before the input data can be used by a FederatedModel.

    For a pipeline, a sequence of transformations, check the FederatedPipeline class.
    """

    features_in: list[QueryFeature] = list()
    features_out: list[QueryFeature] = list()

    random_state: Any = None

    def _columns_in(self) -> list[str]:
        if self.features_in:
            return [f.name for f in self.features_in]
        return list()

    def _columns_out(self) -> list[str]:
        if self.features_out:
            return [f.name for f in self.features_out]
        return list()

    def __eq__(self, other: QueryTransformer) -> bool:
        if not isinstance(other, QueryTransformer):
            return False

        return (
            self.features_in == other.features_in
            and self.features_out == other.features_out
            and self._name == other._name
        )

    def __hash__(self) -> int:
        return hash((self.features_in, self.features_out, self._name))

    def __str__(self) -> str:
        return f"{self._name}({self.features_in} -> {self.features_out})"

    @abstractmethod
    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        """Method used to aggregate multiple transformers trained on different clients.

        Raises:
            NotImplementedError:
                This method need to be implemented (also as empty) by all transformers.
        """

        # TODO: think how to implement this across all transfomers...

        raise NotImplementedError()

    @abstractmethod
    def transform(
        self,
        X_tr: pd.DataFrame | None = None,
        y_tr: pd.DataFrame | None = None,
        X_ts: pd.DataFrame | None = None,
        y_ts: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, Any]:
        """Method used to transform input data in output data. The transformation need to
        be applied on the data, this is always an inplace transformation.

        This basic method for all transformers will both fit and then use the transformer.
        The fitting part will be executed only once. Multiple call to the same transformer
        will apply the already fitted transformer.

        If a transformer need to override this method, remember to check for and assign
        the `self.fitted` field to distinguish between the first call to this method and
        other future calls.

        Args:
            df (pd.DataFrame):
                Input data to be transformed.

        Returns:
            pd.DataFrame:
                The transformed data. The transformation is inplace: in the input `df` param
                and the returned object are are the same.
        """
        raise NotImplementedError()
