from .core import Model, Strategy

from sklearn.ensemble import RandomForestClassifier

import numpy as np
import pickle


class StrategyRandomForestClassifier(Strategy):
    ENSEMBLE = 'ensemble'
    MAJORITY_VOTE = 'majority_vote'


class FederatedRandomForestClassifier(Model):
    name: str = 'FederatedRandomForestClassifier'
    strategy: StrategyRandomForestClassifier | None = None

    # hyper-parameters from scikit-learn
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

    model: RandomForestClassifier | None = None

    def load(self, path) -> None:
        with open(path, 'rb') as f:
            self.model = pickle.load(f)

    def save(self, path) -> None:
        with open(path, 'wb') as f:
            pickle.dump(self.model, f)

    def predict(self, x: np.ndarray) -> np.ndarray:
        return self.model.predict(x)
