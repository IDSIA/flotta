from typing import Any

from ferdelance.schemas.plans.core import GenericPlan, Plan, GenericModel

from sklearn.model_selection import train_test_split

import pandas as pd

import logging
import os

LOGGER = logging.getLogger(__name__)


class TrainTestSplit(GenericPlan):
    def __init__(self, label: str, test_percentage: float = 0.0, random_seed: float | None = None) -> None:
        super().__init__(TrainTestSplit.__name__, label, random_seed)
        self.test_percentage: float = test_percentage

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "test_percentage": self.test_percentage,
        }

    def load(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> None:
        label = self.label
        test_p = self.test_percentage

        self.validate_input(df)

        X_tr = df.drop(label, axis=1).values
        Y_tr = df[label].values

        X_ts, Y_ts = None, None

        if test_p > 0.0:
            X_tr, X_ts, Y_tr, Y_ts = train_test_split(X_tr, Y_tr, test_size=test_p)

        # model training
        local_model.train(X_tr, Y_tr)

        self.path_model = os.path.join(working_folder, f"{artifact_id}_model.pkl")
        local_model.save(self.path_model)

        LOGGER.info(f"saved artifact_id={artifact_id} model to {self.path_model}")

        # model testing
        if X_ts is not None and Y_ts is not None:
            metrics = local_model.eval(X_ts, Y_ts)
            metrics.source = "test"
            metrics.artifact_id = artifact_id

            self.metrics.append(metrics)


class TrainTestValSplit(GenericPlan):
    def __init__(
        self, label: str, test_percentage: float = 0.0, val_percentage: float = 0.0, random_seed: float | None = None
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

    def load(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> None:
        label = self.label
        val_p = self.val_percentage
        test_p = self.test_percentage

        self.validate_input(df)

        X_tr = df.drop(label, axis=1).values
        Y_tr = df[label].values

        X_ts, Y_ts = None, None
        X_val, Y_val = None, None

        if val_p > 0.0:
            X_tr, X_val, Y_tr, Y_val = train_test_split(X_tr, Y_tr, test_size=val_p)

        if test_p > 0.0:
            X_tr, X_ts, Y_tr, Y_ts = train_test_split(X_tr, Y_tr, test_size=test_p)

        # model training
        local_model.train(X_tr, Y_tr)

        self.path_model = os.path.join(working_folder, f"{artifact_id}_model.pkl")
        local_model.save(self.path_model)

        LOGGER.info(f"saved artifact_id={artifact_id} model to {self.path_model}")

        # model testing
        if X_ts is not None and Y_ts is not None:
            metrics = local_model.eval(X_ts, Y_ts)
            metrics.source = "test"
            metrics.artifact_id = artifact_id

            self.metrics.append(metrics)

        # model validation
        if X_val is not None and Y_val is not None:
            metrics = local_model.eval(X_val, Y_val)
            metrics.source = "val"
            metrics.artifact_id = artifact_id

            self.metrics.append(metrics)
