from typing import Any

from ferdelance.schemas.queries import QueryTransformer, QueryFeature

import pandas as pd
import pickle


class Transformer:
    """Basic class that defines a transformer. A transformer is an object that can transform
    input data. This transformation is used as a pre-processing that need to be applied
    before the input data can be used by a FederatedModel.

    For a pipeline, a sequence of transformations, check the FederatedPipeline class.
    """

    def __init__(
        self,
        name: str,
        features_in: QueryFeature | list[QueryFeature] | str | list[str] | None = None,
        features_out: QueryFeature | list[QueryFeature] | str | list[str] | None = None,
        check_for_len: bool = True,
    ) -> None:
        self.name: str = name
        self.features_in: list[str] = convert_features_to_list(features_in)
        self.features_out: list[str] = convert_features_to_list(features_out)

        self.transformer: Any = None

        self.fitted: bool = False

        if check_for_len and len(self.features_in) != len(self.features_out):
            raise ValueError("Input and output features are not of the same length")

    def params(self) -> dict[str, Any]:
        """Utility method to convert to dictionary any input parameter for the transformer.
        This excludes `name`, `features_in`, and `features_out`.

        All classes that extend the Transformer class need to implement this method by
        including the parameters required to build the transformer.

        :return:
            A dictionary with all the input parameters for creating a transformer.
        """
        return dict()

    def dict(self) -> dict[str, Any]:
        """Converts the transformer in a dictionary of its input parameters.

        :return:
            A dictionary with the description of all internal data of a transformer.
        """
        return {
            "name": self.name,
            "features_in": self.features_in,
            "features_out": self.features_out,
            "parameters": self.params(),
        }

    def aggregate(self) -> None:
        """Method used to aggregate multiple transformers trained on different clients."""
        raise NotImplementedError()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Method used to transform input data in output data. The transformation need to
        be applied on the data, this is always an inplace transformation.

        This basic method of all transformers will both fit and then use the transformer.
        The fitting part will be executed only once. Multiple call to the same transformer
        will apply the already fitted transformer.

        If a transformer need to override this method, remember to check for and assign
        the `self.fitted` field to distingue between the first call to this method and
        other future calls.

        :param df:
            Input data to be transformed.
        :return:
            The transformed data. The transformation is inplace: in the input `df` param
            and the returned object are are the same.
        """
        if not self.fitted:
            self.transformer.fit(df[self.features_in])
            self.fitted = True

        df[self.features_out] = self.transformer.transform(df[self.features_in])
        return df

    def build(self) -> QueryTransformer:
        """Convert a Transformer in a QueryTransformer representation that can be sent
        to an aggregation server from a Workbench.

        :return:
            The QueryTransformer representation associated with this transformer.
        """
        return QueryTransformer(**self.dict())

    def __call__(self, df: pd.DataFrame) -> Any:
        return self.transform(df)


def convert_features_to_list(features: QueryFeature | list[QueryFeature] | str | list[str] | None = None) -> list[str]:
    """Sanitize the input list of features in a list of string.

    :param features:
        List of features. These can be a QueryFeature, a list of QueryFeature, a string, ora a list of string.

    :return:
        The input converted in a list of string.
    """
    if features is None:
        return list()
    if isinstance(features, str):
        features = [features]
    elif isinstance(features, QueryFeature):
        features = [features.feature_name]
    elif isinstance(features, list):
        f_list: list[str] = []
        for f in features:
            f_list.append(f.feature_name if isinstance(f, QueryFeature) else f)
        features = f_list
    return features


def save(obj: Transformer, path: str) -> None:
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def load(path: str) -> Any:
    with open(path, "rb") as f:
        return pickle.load(f)
