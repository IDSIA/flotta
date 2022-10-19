from .metrics import Metrics

from typing import Any
from pydantic import BaseModel

import numpy as np

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    roc_auc_score,
    confusion_matrix,
)


class Model(BaseModel):
    """Exchange model description defined in the workbench, trained in 
    the clients, and aggregated in the server.
    """
    name: str
    strategy: str = ''
    parameters: dict[str, Any] = dict()


class GenericModel:
    """This is the class that can manipulate real models."""

    def load(self, path) -> None:
        raise NotImplementedError()

    def save(self, path) -> None:
        raise NotImplementedError()

    def train(self, x: np.ndarray, y: np.ndarray) -> None:
        raise NotImplementedError()

    def predict(self, x: np.ndarray) -> np.ndarray:
        raise NotImplementedError()

    def eval(self, x: np.ndarray, y: np.ndarray) -> Metrics:
        y_prob = self.predict(x)
        y_pred = (y_prob > 0.5)

        cf = confusion_matrix(y, y_pred)

        return Metrics(
            accuracy_score=float(accuracy_score(y, y_pred)),
            precision_score=float(precision_score(y, y_pred)),
            recall_score=float(recall_score(y, y_pred)),
            auc_score=float(roc_auc_score(y, y_prob)),
            confusion_matrix_TP=cf[0, 0],
            confusion_matrix_FN=cf[0, 1],
            confusion_matrix_FP=cf[1, 0],
            confusion_matrix_TN=cf[1, 1],
        )

    def build(self) -> Model:
        raise NotImplementedError()
