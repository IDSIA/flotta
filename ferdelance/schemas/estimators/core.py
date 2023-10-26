from __future__ import annotations
from typing import Any
from abc import ABC, abstractmethod

from ferdelance.core.queries.features import QueryFeature
from ferdelance.core.utils import convert_features_in_to_list

from pydantic import BaseModel

import pandas as pd


class Estimator(BaseModel):
    name: str
    features_in: list[QueryFeature]
    random_state: Any
    params: dict[str, Any]


class GenericEstimator(ABC):
    def __init__(
        self,
        name: str,
        features_in: QueryFeature | list[QueryFeature] | None = None,
        random_state: Any = None,
    ) -> None:
        self.name: str = name
        self.features_in: list[QueryFeature] = convert_features_in_to_list(features_in)
        self.random_state: Any = random_state

        self.estimator: Any = None

        self._columns_in: list[str] = [f.name for f in self.features_in]

    def params(self) -> dict[str, Any]:
        return dict()

    def build(self) -> Estimator:
        return Estimator(
            name=self.name,
            features_in=self.features_in,
            random_state=self.random_state,
            params=self.params(),
        )

    @abstractmethod
    def initialize(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def fit(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()

    @abstractmethod
    def aggregate(self, estimator_a: GenericEstimator, estimator_b: GenericEstimator) -> Estimator:
        """Merge two estimators together. A new estimator need to be created.
        If an issue occurs, raise ValueError exception.

        Args:
            estimator_a (Estimator):
                The estimator on the left.
            estimator_b (Estimator):
                The estimator on the right.

        Raises:
            NotImplementedError:
                Raised if this method is not implemented.
            ValueError:
                Raised when an error occurs during the aggregation.

        Returns:
            Estimator:
                A new estimator, aggregation of the two inputs.
        """
        raise NotImplementedError()

    @abstractmethod
    def finalize(self, estimator: GenericEstimator) -> None:
        raise NotImplementedError()

    @abstractmethod
    def estimate(self, df: pd.DataFrame) -> Any:
        raise NotImplementedError()

    def __call__(self, df: pd.DataFrame) -> Any:
        return self.estimate(df)
