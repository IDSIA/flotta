from .core import GenericModel, Model, Strategy
from pydantic import BaseModel

from sklearn.ensemble import RandomForestClassifier
from typing import Any

import numpy as np
import pickle


class StrategyRandomForestClassifier(Strategy):

    MERGE = 'merge'
    """The trees generated by all clients will be merged in a single RandomForestClassifier"""

    MAJORITY_VOTE = 'majority_vote'
    """All the models will be put together and a classification is decided by majority classification between all models."""


class ParametersRandomForestClassifier(BaseModel):
    """
    Class defining all the parameters accepted in training by the model.

    These parameters are default values from scikit-learn.

    For more details, please refer to https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html 
    """
    n_estimators: int = 100
    criterion: str = 'gini'
    max_depth: int | None = None
    min_samples_split: int = 2
    min_samples_leaf: int = 1
    min_weight_fraction_leaf: float = 0
    max_features: str = 'sqrt'
    max_leaf_nodes: int | None = None
    min_impurity_decrease: float = 0
    bootstrap: bool = True
    oob_score: bool = False
    n_jobs: int | None = None
    random_state: int | None = None
    class_weight: str | list[str] | list[dict[str, float]] | None = None
    ccp_alpha: float = 0
    max_samples: int | None = None


class FederatedRandomForestClassifier(GenericModel):
    name: str = 'FederatedRandomForestClassifier'
    strategy: StrategyRandomForestClassifier
    parameters: dict[str, Any]

    model: RandomForestClassifier | None = None

    def __init__(self, model_desc: Model) -> None:
        self.strategy = StrategyRandomForestClassifier[model_desc.strategy]
        self.parameters = ParametersRandomForestClassifier(**model_desc.parameters)

    def __init__(self, **data):
        data['parameters'] = data['parameters'].dict()
        super().__init__(**data)

    def strategy(self) -> StrategyRandomForestClassifier:
        return StrategyRandomForestClassifier[self.strategy]

    def load(self, path) -> None:
        with open(path, 'rb') as f:
            self.model = pickle.load(f)

    def save(self, path) -> None:
        with open(path, 'wb') as f:
            pickle.dump(self.model, f)

    def predict(self, x: np.ndarray) -> np.ndarray:
        return self.model.predict(x)
