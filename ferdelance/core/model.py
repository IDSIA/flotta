from __future__ import annotations
from typing import Any, Sequence
from abc import ABC, abstractmethod

from pydantic import PrivateAttr

from ferdelance.core.entity import Entity
from ferdelance.core.metrics import Metrics
from ferdelance.core.interfaces import Step
from ferdelance.core.queries import Query

from numpy.typing import ArrayLike

import numpy as np
import pickle

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    roc_auc_score,
    confusion_matrix,
)


class Model(ABC, Entity):
    """This is the class that can manipulate real models. The client and the
    server will run the code implemented by classes that extends this one."""

    query: Query | None = None

    _model: Any = PrivateAttr()

    def load(self, path: str) -> None:
        """Load a trained model from a path on the local disk to the internal
        model object. The loaded model must be in pickle format.

        :param path:
            A valid path to a model downloaded from the aggregation server.
        """
        with open(path, "rb") as f:
            self._model = pickle.load(f)

    def set_model(self, model: Any) -> None:
        self._model = model

    def get_model(self) -> Any:
        return self._model

    def save(self, path: str) -> None:
        """Save the internal model object to the disk. Models save with this method
        can be loaded again using the `load()` method.

        :param path:
            A valid path to the disk.
        """
        with open(path, "wb") as f:
            pickle.dump(self._model, f)

    def bin(self) -> bytes:
        return pickle.dumps(self._model)

    @abstractmethod
    def train(self, x, y) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def aggregate(self, model_a, model_b) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def predict(self, x) -> np.ndarray:
        """Predict the probabilities for the given instances of features.

        :param x:
            Values with features to predict the target labels.
        """
        raise NotImplementedError()

    @abstractmethod
    def classify(self, x) -> np.ndarray | ArrayLike:
        """Predict the classes for the given instances of features.

        :param x:
            Values with features to predict the target labels.
        """
        raise NotImplementedError()

    def eval(self, x, y) -> Metrics:
        """Perform some evaluations and compute metrics given the input data.

        :param x:
            Values with features created from a dataset to evaluate.
        :param y:
            True target labels values aligned with the feature values.
        """
        y_prob = self.predict(x)
        y_pred = self.classify(x)

        cf = confusion_matrix(y, y_pred)

        return Metrics(
            accuracy_score=float(accuracy_score(y, y_pred)),
            precision_score=float(precision_score(y, y_pred)),
            recall_score=float(recall_score(y, y_pred)),
            auc_score=float(roc_auc_score(y, y_prob[:, 1])),
            confusion_matrix_TP=cf[0, 0],
            confusion_matrix_FN=cf[0, 1],
            confusion_matrix_FP=cf[1, 0],
            confusion_matrix_TN=cf[1, 1],
        )

    @abstractmethod
    def get_steps(self) -> Sequence[Step]:
        raise NotImplementedError()
