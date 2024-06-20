from __future__ import annotations
from typing import Any
from abc import ABC, abstractmethod

from ferdelance.core.entity import Entity
from ferdelance.core.environment import Environment
from ferdelance.core.queries import QueryFeature
from ferdelance.core.utils import convert_features_in_to_list, convert_features_out_to_list

from pydantic import SerializeAsAny


class QueryTransformer(ABC, Entity):
    """Basic class that defines a transformer. A transformer is an object that can transform
    input data. This transformation is used as a pre-processing that need to be applied
    before the input data can be used by a FederatedModel.

    For a pipeline, a sequence of transformations, check the FederatedPipeline class.
    """

    features_in: list[QueryFeature] = list()
    features_out: list[QueryFeature] = list()

    def __init__(
        self,
        features_in: QueryFeature | list[QueryFeature] | str | list[str] | None = None,
        features_out: QueryFeature | list[QueryFeature] | str | list[str] | None = None,
        random_state: Any = None,
        **data,
    ):
        super(QueryTransformer, self).__init__(random_state=random_state, **data)

        self.features_in = convert_features_in_to_list(features_in)
        self.features_out = convert_features_out_to_list(self.features_in, features_out)

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
            and self.entity == other.entity
        )

    def __hash__(self) -> int:
        return hash((self.features_in, self.features_out, self.entity))

    def __str__(self) -> str:
        return f"{self.entity}({self.features_in} -> {self.features_out})"

    @abstractmethod
    def aggregate(self, env: Environment) -> Environment:
        """Method used to aggregate multiple transformers trained on different clients.

        Raises:
            NotImplementedError:
                This method need to be implemented (also as empty) by all transformers.
        """

        # TODO: think how to implement this across all transformers...

        raise NotImplementedError()

    @abstractmethod
    def transform(self, env: Environment) -> tuple[Environment, Any]:
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


TQueryTransformer = SerializeAsAny[QueryTransformer]
