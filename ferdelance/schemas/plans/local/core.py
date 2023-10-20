from __future__ import annotations
from typing import Any

from ferdelance.logging import get_logger
from ferdelance.schemas.models import GenericModel, Metrics
from ferdelance.schemas.plans.plan import Plan, Plan, PlanResult


import pandas as pd

import json

LOGGER = get_logger(__name__)


class LocalPlan(Plan):
    """Describe how to train and evaluate a model based on the input data source."""

    def __init__(self, name: str, label: str, random_seed: Any = None, local_plan: LocalPlan | None = None) -> None:
        super().__init__(name, random_seed)
        self.label: str = label

        self.path_model: str | None = None

        self.local_plan: LocalPlan | None = local_plan

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "label": self.label,
        }

    def build(self) -> Plan:
        """Converts the GenericPlan instance to a Plan exchange object.

        Returns:
            Plan:
                Object that can be sent to a server or a client in JSON format.
        """
        if self.local_plan is not None:
            return Plan(
                name=self.name,
                params=self.params(),
                plan=self.local_plan.build(),
            )
        return super().build()

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

    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> PlanResult:
        return super().run(df, local_model, working_folder, artifact_id)
