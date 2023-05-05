from typing import Any
from ferdelance.schemas.models import GenericModel

from ferdelance.schemas.plans.core import GenericPlan, GenericModel

import pandas as pd

import logging
import os

LOGGER = logging.getLogger(__name__)


class IterativePlan(GenericPlan):
    def __init__(
        self,
        label: str,
        local_plan: GenericPlan,
        iterations: int = -1,
        random_seed: float | None = None,
    ) -> None:
        super().__init__(IterativePlan.__name__, label, random_seed, local_plan)

        self.iterations: int = iterations

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "iterations": self.iterations,
        }

    def load(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> None:
        if self.local_plan is None:
            raise ValueError("No local plan defined!")

        self.local_plan.load(df, local_model, working_folder, artifact_id)
