from __future__ import annotations
from typing import Any, Sequence
from abc import ABC, abstractmethod

from ferdelance.core.entity import Entity
from ferdelance.core.interfaces import Step
from ferdelance.core.queries.features import QueryFeature
from ferdelance.core.utils import convert_features_in_to_list


import pandas as pd


class Estimator(ABC, Entity):
    features_in: list[QueryFeature]
    random_state: Any

    def __init__(
        self,
        features_in: QueryFeature | list[QueryFeature] | str | list[str] | None = None,
        random_state: Any = None,
        **data,
    ):
        super(Estimator, self).__init__(
            features_in=convert_features_in_to_list(features_in),  # type: ignore
            random_state=random_state,  # type: ignore
            **data,
        )

    @abstractmethod
    def get_steps(self) -> Sequence[Step]:
        raise NotImplementedError()

    @abstractmethod
    def initialize(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def fit(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()

    @abstractmethod
    def aggregate(self, estimator_a: Estimator, estimator_b: Estimator) -> Estimator:
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
    def finalize(self, estimator: Estimator) -> None:
        raise NotImplementedError()

    @abstractmethod
    def estimate(self, df: pd.DataFrame) -> Any:
        raise NotImplementedError()

    def __call__(self, df: pd.DataFrame) -> Any:
        return self.estimate(df)
