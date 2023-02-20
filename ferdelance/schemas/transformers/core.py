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
        features_in: QueryFeature | list[QueryFeature] | None = None,
        features_out: QueryFeature | list[QueryFeature] | str | list[str] | None = None,
        check_for_len: bool = True,
    ) -> None:
        """Creates a transformer and assigns the input features so that they are coherent with the framework.

        Args:
            name (str):
                Name of the transformer (usually, class.__name__ is enough).
            features_in (QueryFeature | list[QueryFeature] | None, optional):
                List of features used as input. If the transformer does not have input values, set to None.
                If a single value is passed, it will be converted to a list of one single element.
                Defaults to None.
            features_out (QueryFeature | list[QueryFeature] | str | list[str] | None, optional):
                List of features used as output. If None is passed, then the output features will be a copy of
                the features_in parameter. If a string or a list of string is passed, then a new list of outputs
                will be generated with the given name(s) and the same dtype as features_in (where possible).
                Defaults to None.
            check_for_len (bool, optional):
                If set to True, an exception is raised when the length of the features_in and features_out is
                not the same.
                Defaults to True.

        Raises:
            ValueError:
                raise if the check_for_len parameter is set to True and the feature_in and features_out parameters
                does not have the same length.
        """
        self.name: str = name
        self.features_in: list[QueryFeature] = convert_features_in_to_list(features_in)
        self.features_out: list[QueryFeature] = convert_features_out_to_list(
            self.features_in, features_out, check_for_len
        )

        self.transformer: Any = None

        self.fitted: bool = False

        self._columns_in: list[str] = [f.name for f in self.features_in]
        self._columns_out: list[str] = [f.name for f in self.features_out]

        if check_for_len and len(self.features_in) != len(self.features_out):
            raise ValueError("Input and output features are not of the same length")

    def params(self) -> dict[str, Any]:
        """Utility method to convert to dictionary any input parameter for the transformer.
        This excludes `name`, `features_in`, and `features_out`.

        All classes that extend the Transformer class need to implement this method by
        including the parameters required to build the transformer.

        Returns:
            dict[str, Any]:
                A dictionary with all the input parameters for creating a transformer.
        """
        return dict()

    def dict(self) -> dict[str, Any]:
        """Converts the transformer in a dictionary of its input parameters.

        Returns:
            dict[str, Any]:
                A dictionary with the description of all internal data of a transformer.
        """
        return {
            "name": self.name,
            "features_in": self.features_in,
            "features_out": self.features_out,
            "parameters": self.params(),
        }

    def aggregate(self) -> None:
        """Method used to aggregate multiple transformers trained on different clients.

        Raises:
            NotImplementedError:
                This method need to be implemented (also as empty) by all transformers.
        """
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

        Args:
            df (pd.DataFrame):
                Input data to be transformed.

        Returns:
            pd.DataFrame:
                The transformed data. The transformation is inplace: in the input `df` param
                and the returned object are are the same.
        """
        if not self.fitted:
            self.transformer.fit(df[self._columns_in])
            self.fitted = True

        df[self._columns_out] = self.transformer.transform(df[self._columns_in])
        return df

    def build(self) -> QueryTransformer:
        """Convert a Transformer in a QueryTransformer representation that can be sent
        to an aggregation server from a Workbench.

        Returns:
            QueryTransformer:
                The QueryTransformer representation associated with this transformer.
        """
        return QueryTransformer(**self.dict())

    def __call__(self, df: pd.DataFrame) -> Any:
        return self.transform(df)


def convert_features_in_to_list(features_in: QueryFeature | list[QueryFeature] | None = None) -> list[QueryFeature]:
    """Sanitize the input list of features in a list of QueryFeature.

    Args:
        features_in (QueryFeature | list[QueryFeature] | None, optional):
            List of features. These can be a QueryFeature, a list of QueryFeature, or None.
            Defaults to None.

    Returns:
        list[QueryFeature]:
            The input converted in a list of QueryFeatures.
    """
    if features_in is None:
        return list()

    if isinstance(features_in, QueryFeature):
        features_in = [features_in]

    return features_in


def convert_features_out_to_list(
    features_in: list[QueryFeature],
    features_out: QueryFeature | list[QueryFeature] | str | list[str] | None = None,
    check_len: bool = True,
) -> list[QueryFeature]:
    """Sanitize the output list of features in a list of QueryFeature.

    Args:
        features_in (list[QueryFeature]): _description_
        features_out (QueryFeature | list[QueryFeature] | str | list[str] | None, optional):
            List of features used as output. If None is passed, then the output features will be a copy of
            the features_in parameter. If a string or a list of string is passed, then a new list of outputs
            will be generated with the given name(s) and the same dtype as features_in (where possible). If
            an empty list is passed, it will be returned another empty list.
            Defaults to None.

    Returns:
        list[QueryFeature]:
            The outputs converted in a list of QueryFeatures.

    Raises:
        ValueError:
    """

    if features_out is None:
        return features_in.copy()

    if isinstance(features_out, str):
        if len(features_in) != 1:
            raise ValueError("Multiple input features but only one feature as output.")
        return [QueryFeature(name=features_out, dtype=features_in[0].name)]

    if isinstance(features_out, QueryFeature):
        return [features_out]

    if isinstance(features_out, list):
        if len(features_out) == 0:
            return list()

        if check_len and len(features_in) != len(features_out):
            raise ValueError("Different number of input features and output features.")

        ret: list[QueryFeature] = list()

        for f_in, f_out in zip(features_in, features_out):
            if isinstance(f_out, QueryFeature):
                ret.append(f_out)
            else:
                ret.append(QueryFeature(name=f_out, dtype=f_in.dtype))

        return ret

    raise ValueError("Unsupported features_output parameter type.")


def save(obj: Transformer, path: str) -> None:
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def load(path: str) -> Any:
    with open(path, "rb") as f:
        return pickle.load(f)
