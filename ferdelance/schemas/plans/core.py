from __future__ import annotations
from typing import Any
from abc import ABC, abstractmethod

from ferdelance.logging import get_logger
from ferdelance.schemas.models import GenericModel, Metrics
from ferdelance.schemas.context import TaskAggregationContext

from pydantic import BaseModel

import pandas as pd

import json

LOGGER = get_logger(__name__)


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

        self.path_model: str | None = None

        self.local_plan: GenericPlan | None = local_plan

    def params(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "random_seed": self.random_seed,
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
                local_plan=self.local_plan.build(),
            )
        return Plan(
            name=self.name,
            params=self.params(),
        )

    async def pre_aggregation_hook(self, context: TaskAggregationContext) -> None:
        """This hook controls the start of an aggregation job."""
        pass

    async def post_aggregation_hook(self, context: TaskAggregationContext) -> None:
        """This hook controls the scheduling of new training jobs."""
        pass

    @abstractmethod
    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> list[Metrics]:
        """Method executed by each client. Implement this method to specify what a client need to do to build and
        evaluate a local model.

        Args:
            df (pd.DataFrame):
                Data to work on from the previous extraction query.
            local_model (GenericModel):
                Description of the local model to build.
            working_folder (str):
                Working folder to use
            artifact_id (str):
                Id of the artifact that will be executed

        Raises:
            NotImplementedError:
                If the plan does not implement this method.
        """
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
