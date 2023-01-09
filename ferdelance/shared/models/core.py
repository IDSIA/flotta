from __future__ import annotations
from .metrics import Metrics

from typing import Any
from pydantic import BaseModel

import numpy as np
import pickle

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

    def load(self, path: str) -> None:
        """Load a trained model from a path on the local disk to the internal
        model object.

        :param path:
            A valid path to a model downloaded from the aggregation server.
        """
        with open(path, 'rb') as f:
            self.model = pickle.load(f)

    def save(self, path: str) -> None:
        """Save the internal model object to the disk. Models save with this method
        can be loaded again using the `load()` method.

        :param path:
            A valid path to the disk. 
        """
        with open(path, 'wb') as f:
            pickle.dump(self.model, f)

    def train(self, x, y) -> None:
        """Perform a training using local data produced with a Query, a Pipeline, or a Dataset.
        This method is used by the clients to train the partial models.

        :param x:
            Values with features created from a dataset.
        :param y:
            Target labels aligned with the feature values.
        """
        raise NotImplementedError()

    def aggregate(self, strategy: str, model_a, model_b):
        """Aggregates two models and produces a new one, following the given strategy. This aggregation
        function is called from the workers on the server that perform aggregations.
        Aggregations are done between two partial models, producing a new aggregated model. This 
        aggregated model is considered a partial model until all models have been aggregated.

        Keep in mind that this aggregation function will be called upon a list of partial models:
        the first model can be a partial model or an already partial-aggregated model.

        :param strategy:
            Name of the strategy to use.
        :param model_a:
            Partial model to aggregate with. This can also be an already aggregated model.
        :param model_b:
            Partial model to aggregate with. This is a new model from a client.
        """
        raise NotImplementedError()

    def predict(self, x) -> np.ndarray:
        """Predict the probabilities for the given instances of features.

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
        """Convert this GenericModel to a Model that can be sent to the aggregation server
        for the federate learning training procedure.
        """
        raise NotImplementedError()