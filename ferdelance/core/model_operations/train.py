from __future__ import annotations
from typing import Any

from ferdelance.logging import get_logger
from ferdelance.core.models.core import Model
from ferdelance.core.model_operations import ModelOperation

import pandas as pd


LOGGER = get_logger(__name__)


class Train(ModelOperation):
    """Execution plan that train a model over all the available data.
    No evaluation step is performed.
    """

    model: Model
    label: str
    random_seed: Any

    def run(self, env: dict[str, Any]) -> dict[str, Any]:
        df: pd.DataFrame = env["df"]
        artifact_id: str = env["artifact_id"]

        label = self.label

        self.validate_input(df)

        X_tr = df.drop(label, axis=1).values
        Y_tr = df[label].values

        # model training
        env["local_model"] = self.model.train(X_tr, Y_tr)

        LOGGER.info(f"artifact={artifact_id}: local model train completed")

        return env
