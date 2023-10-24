from __future__ import annotations
from typing import Any

from ferdelance.logging import get_logger
from ferdelance.core.metrics import Metrics
from ferdelance.core.models.core import Model
from ferdelance.core.operations.core import Operation
from ferdelance.core.queries.core import Query


import pandas as pd

import json


LOGGER = get_logger(__name__)


class ModelOperation(Operation):
    """Describe how to train and evaluate a model based on the input data source."""

    query: Query
    label: str
    model: Model
    random_seed: Any = None

    def validate_input(self, df: pd.DataFrame) -> None:
        if self.label is None:
            msg = "label is not defined!"
            LOGGER.error(msg)
            raise ValueError(msg)

        if self.label not in df.columns:
            msg = f"label {self.label} not found in data source!"
            LOGGER.error(msg)
            raise ValueError(msg)

    def store_metrics(self, metrics: Metrics, path: str) -> None:
        with open(path, "w") as f:
            content = json.dumps(metrics)
            f.write(content)

    def exec(self, env: dict[str, Any]) -> dict[str, Any]:
        env["df"] = self.query.run()
        return env
