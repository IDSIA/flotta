from typing import Any

from ferdelance.config import get_logger
from ferdelance.schemas.plans.core import GenericPlan, GenericModel, Metrics

from sklearn.model_selection import train_test_split

import pandas as pd

import os

LOGGER = get_logger(__name__)


class TrainAll(GenericPlan):
    """Execution plan that train a model over all the available data.
    No evaluation step is performed.
    """

    def __init__(self, label: str, random_seed: float | None = None) -> None:
        super().__init__(TrainAll.__name__, label, random_seed)

    def params(self) -> dict[str, Any]:
        return super().params() | {}

    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> list[Metrics]:
        label = self.label

        self.validate_input(df)

        X_tr = df.drop(label, axis=1).values
        Y_tr = df[label].values

        # model training
        local_model.train(X_tr, Y_tr)

        self.path_model = os.path.join(working_folder, f"{artifact_id}_model.pkl")
        local_model.save(self.path_model)

        LOGGER.info(f"artifact_id={artifact_id}: saved model to {self.path_model}")

        return list()


class TrainTestSplit(GenericPlan):
    """Execution plan that train a model on a percentage of available data and test it on the remaining part."""

    def __init__(self, label: str, test_percentage: float = 0.0, random_seed: Any = None) -> None:
        super().__init__(TrainTestSplit.__name__, label, random_seed)
        self.test_percentage: float = test_percentage

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "test_percentage": self.test_percentage,
        }

    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> list[Metrics]:
        label = self.label
        test_p = self.test_percentage

        self.validate_input(df)

        X_tr = df.drop(label, axis=1).values
        Y_tr = df[label].values

        X_ts, Y_ts = None, None

        if test_p > 0.0:
            X_tr, X_ts, Y_tr, Y_ts = train_test_split(X_tr, Y_tr, test_size=test_p, random_state=self.random_seed)

        # model training
        local_model.train(X_tr, Y_tr)

        self.path_model = os.path.join(working_folder, f"{artifact_id}_model.pkl")
        local_model.save(self.path_model)

        LOGGER.info(f"artifact_id={artifact_id}: saved model to {self.path_model}")

        # model testing
        metrics_list: list[Metrics] = list()
        if X_ts is not None and Y_ts is not None:
            metrics = local_model.eval(X_ts, Y_ts)
            metrics.source = "test"
            metrics.artifact_id = artifact_id

            metrics_list.append(metrics)

        return metrics_list


class TrainTestValSplit(GenericPlan):
    """Execution plan that splits in three parts the available data. One part will be used to train a model, the second
    one to test it, and the final one to validate the model.
    """

    def __init__(
        self, label: str, test_percentage: float = 0.0, val_percentage: float = 0.0, random_seed: Any = None
    ) -> None:
        super().__init__(TrainTestValSplit.__name__, label, random_seed)
        self.test_percentage: float = test_percentage
        self.val_percentage: float = val_percentage

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "test_percentage": self.test_percentage,
            "val_percentage": self.val_percentage,
        }

    test_percentage: float = 0.0
    val_percentage: float = 0.0

    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> list[Metrics]:
        label = self.label
        val_p = self.val_percentage
        test_p = self.test_percentage

        self.validate_input(df)

        X_tr = df.drop(label, axis=1).values
        Y_tr = df[label].values

        X_ts, Y_ts = None, None
        X_val, Y_val = None, None

        if val_p > 0.0:
            X_tr, X_val, Y_tr, Y_val = train_test_split(X_tr, Y_tr, test_size=val_p, random_state=self.random_seed)

        if test_p > 0.0:
            X_tr, X_ts, Y_tr, Y_ts = train_test_split(X_tr, Y_tr, test_size=test_p, random_state=self.random_seed)

        # model training
        local_model.train(X_tr, Y_tr)

        self.path_model = os.path.join(working_folder, f"{artifact_id}_model.pkl")
        local_model.save(self.path_model)

        LOGGER.info(f"artifact_id={artifact_id}: saved model to {self.path_model}")

        list_metrics: list[Metrics] = list()

        # model testing
        if X_ts is not None and Y_ts is not None:
            metrics = local_model.eval(X_ts, Y_ts)
            metrics.source = "test"
            metrics.artifact_id = artifact_id

            list_metrics.append(metrics)

        # model validation
        if X_val is not None and Y_val is not None:
            metrics = local_model.eval(X_val, Y_val)
            metrics.source = "val"
            metrics.artifact_id = artifact_id

            list_metrics.append(metrics)

        return list_metrics
