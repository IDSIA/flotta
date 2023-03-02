__all__ = [
    "Model",
    "GenericModel",
    "Metrics",
    "FederatedRandomForestClassifier",
    "StrategyRandomForestClassifier",
    "ParametersRandomForestClassifier",
    "rebuild_model",
]

from .core import (
    Model,
    GenericModel,
)
from .metrics import Metrics
from .federated_random_forest_classifier import (
    FederatedRandomForestClassifier,
    StrategyRandomForestClassifier,
    ParametersRandomForestClassifier,
)


def rebuild_model(model: Model) -> GenericModel:
    if model.name == FederatedRandomForestClassifier.__name__:
        return FederatedRandomForestClassifier(
            strategy=StrategyRandomForestClassifier[model.strategy],
            parameters=ParametersRandomForestClassifier(**model.parameters),
        )

    raise ValueError(f"model={model.name} is not supported")
