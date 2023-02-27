from typing import Any

from ferdelance.schemas.queries import QueryFeature, QueryEstimator
from ferdelance.schemas.utils import convert_features_in_to_list

import pandas as pd
import pickle


class Estimator:
    def __init__(self, name: str, features_in: QueryFeature | list[QueryFeature] | None = None) -> None:
        self.name: str = name
        self.features_in: list[QueryFeature] = convert_features_in_to_list(features_in)

        self.estimator: Any = None

        self.fitted: bool = False

        self._columns_in: list[str] = [f.name for f in self.features_in]

    def params(self) -> dict[str, Any]:
        return dict()

    def dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "features_in": self.features_in,
            "parameters": self.params(),
        }

    def estimate(self, df: pd.DataFrame) -> float:
        raise NotImplementedError()

    def build(self) -> QueryEstimator:
        return QueryEstimator(**self.dict())

    def __call__(self, df: pd.DataFrame) -> Any:
        return self.estimate(df)


def save(obj: Estimator, path: str) -> None:
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def load(path: str) -> Estimator:
    with open(path, "rb") as f:
        return pickle.load(f)
