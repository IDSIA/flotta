from __future__ import annotations
from ferdelance_shared.schemas.models import (
    Model,
    FederatedRandomForestClassifier as BaseModel,
    StrategyRandomForestClassifier,
    ParametersRandomForestClassifier,
)

from sklearn.ensemble import RandomForestClassifier

import numpy as np


class LocalFederatedRandomForestClassifier(BaseModel):

    def __init__(self, strategy: StrategyRandomForestClassifier, parameters: ParametersRandomForestClassifier) -> None:
        super().__init__(strategy, parameters)

        self.model = RandomForestClassifier(**self.parameters.dict())

    def train(self, x: np.ndarray, y: np.ndarray) -> None:
        if self.model is None:
            self.model = RandomForestClassifier(**self.parameters.dict())

        self.model.fit(x, y)

    @staticmethod
    def from_model(model: Model) -> LocalFederatedRandomForestClassifier:
        return LocalFederatedRandomForestClassifier(
            strategy=StrategyRandomForestClassifier[model.strategy],
            parameters=ParametersRandomForestClassifier(**model.parameters),
        )
