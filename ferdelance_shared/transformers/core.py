from typing import Any

from ..artifacts.queries import QueryTransformer, QueryFeature

from io import BufferedReader

import pandas as pd
import pickle


class Transformer:

    def __init__(self, name: str, features_in: QueryFeature | list[QueryFeature] | str | list[str] | None = None, features_out: QueryFeature | list[QueryFeature] | str | list[str] | None = None) -> None:
        self.name: str = name
        self.features_in: list[str] = convert_features_to_list(features_in)
        self.features_out: list[str] = convert_features_to_list(features_out)

        if len(self.features_in) != len(self.features_out):
            raise ValueError('Input and output features are not of the same length')

    def params(self) -> dict[str, Any]:
        return dict()

    def dict(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'features_in': self.features_in,
            'features_out': self.features_out,
            'parameters': self.params(),
        }

    def aggregate(self) -> None:
        raise NotImplementedError()

    def fit(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    def build(self) -> QueryTransformer:
        return QueryTransformer(**self.dict())


def convert_features_to_list(features: QueryFeature | list[QueryFeature] | str | list[str] | None = None) -> list[str]:
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
    with open(path, 'wb') as f:
        pickle.dump(obj, f)


def load(path: str) -> Any:
    with open(path, 'rb') as f:
        return pickle.load(f)
