from typing import Any
from abc import ABC, abstractmethod

from ferdelance.core.entity import Entity
from ferdelance.core.queries import QueryFeature
from ferdelance.schemas.utils import convert_features_in_to_list, convert_features_out_to_list

from pydantic import validator


class Transformer(ABC, Entity):
    """Basic class that defines a transformer. A transformer is an object that can transform
    input data. This transformation is used as a pre-processing that need to be applied
    before the input data can be used by a FederatedModel.

    For a pipeline, a sequence of transformations, check the FederatedPipeline class.
    """

    features_in: list[QueryFeature] | None = None
    features_out: list[QueryFeature] | None = None

    _columns_in: list[str]
    _columns_out: list[str]

    transformer: Any = None

    @validator("features_in")
    def convert_features_in_to_list(cls, values):
        features_in: list[QueryFeature] = convert_features_in_to_list(values["features_in"])
        _columns_in: list[str] = [f.name for f in features_in]

        values["features_in"] = features_in
        values["_columns_in"] = _columns_in

        return values

    @validator("features_out")
    def convert_features_out_to_list(cls, values):
        features_out: list[QueryFeature] = convert_features_out_to_list(values["features_in"])
        _columns_out: list[str] = [f.name for f in features_out]

        values["features_out"] = features_out
        values["_columns_out"] = _columns_out

        return values

    @abstractmethod
    def get_transformer(self) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def fit(self, env: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError()

    @abstractmethod
    def aggregate(self, env: dict[str, Any]) -> dict[str, Any]:
        """Method used to aggregate multiple transformers trained on different clients.

        Raises:
            NotImplementedError:
                This method need to be implemented (also as empty) by all transformers.
        """
        raise NotImplementedError()

    def transform(self, env: dict[str, Any]) -> dict[str, Any]:
        """Method used to transform input data in output data. The transformation need to
        be applied on the data, this is always an inplace transformation.

        This basic method of all transformers will both fit and then use the transformer.
        The fitting part will be executed only once. Multiple call to the same transformer
        will apply the already fitted transformer.

        If a transformer need to override this method, remember to check for and assign
        the `self.fitted` field to distingue between the first call to this method and
        other future calls.

        Args:
            df (pd.DataFrame):
                Input data to be transformed.

        Returns:
            pd.DataFrame:
                The transformed data. The transformation is inplace: in the input `df` param
                and the returned object are are the same.
        """
        df = env["df"]
        transformer = env.get("transformer", self.get_transformer())

        if not self.fitted:
            transformer.fit(df[self._columns_in])
            env["transformer"] = transformer
            self.fitted = True

        df[self._columns_out] = transformer.transform(df[self._columns_in])

        env["df"] = df

        return env

    def __call__(self, env: dict[str, Any]) -> dict[str, Any]:
        return self.transform(env)
