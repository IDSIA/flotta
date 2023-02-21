from ferdelance.schemas.models import GenericModel, Metrics

from pydantic import BaseModel

import pandas as pd

import json
import logging

LOGGER = logging.getLogger(__name__)


class LoadingPlan(BaseModel):
    """Describe how to train and evaluate a model based on the input data source."""

    label: str

    random_seed: float | None = None

    _metrics: list[Metrics] = list()
    _path_model: str | None = None

    def load(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> None:
        raise NotImplementedError()

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
