from __future__ import annotations

from pydantic import SerializeAsAny

from ferdelance.core.metrics import Metrics
from ferdelance.core.model import TModel as Model
from ferdelance.core.operations import QueryOperation
from ferdelance.logging import get_logger

from pathlib import Path

import json


LOGGER = get_logger(__name__)


class ModelOperation(QueryOperation):
    """Describe how to train and evaluate a model based on the input data source."""

    model: Model

    def store_metrics(self, metrics: Metrics, path: Path) -> None:
        with open(path, "w") as f:
            content = json.dumps(metrics, indent=True)
            f.write(content)


TModelOperation = SerializeAsAny[ModelOperation]
