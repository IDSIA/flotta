from __future__ import annotations
from typing import Any
from abc import ABC, abstractmethod

from ferdelance.schemas.models import GenericModel, Metrics

from pydantic import BaseModel

import pandas as pd

import json
import logging

LOGGER = logging.getLogger(__name__)


class Plan(BaseModel):
    name: str
    params: dict[str, Any]
    local_plan: Plan | None = None


class GenericPlan(ABC):
    """Describe how to train and evaluate a model based on the input data source."""

    def __init__(self, name: str, label: str, random_seed: Any = None, local_plan: GenericPlan | None = None) -> None:
        self.name: str = name
        self.label: str = label
        self.random_seed: Any = random_seed

        self.metrics: list[Metrics] = list()
        self.path_model: str | None = None

        self.local_plan: GenericPlan | None = local_plan

    def params(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "random_seed": self.random_seed,
        }

    def build(self) -> Plan:
        if self.local_plan is not None:
            return Plan(
                name=self.name,
                params=self.params(),
                local_plan=self.local_plan.build(),
            )
        return Plan(
            name=self.name,
            params=self.params(),
        )

    @abstractmethod
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
