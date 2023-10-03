from __future__ import annotations
from typing import Any

from ferdelance.logging import get_logger
from ferdelance.schemas.models import GenericModel, Metrics
from ferdelance.schemas.plans.local.core import LocalPlan

import os
import pandas as pd

LOGGER = get_logger(__name__)


class TrainAll(LocalPlan):
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

        LOGGER.info(f"artifact={artifact_id}: saved model to {self.path_model}")

        return list()
