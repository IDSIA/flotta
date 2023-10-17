from __future__ import annotations
from abc import abstractmethod
from typing import Any

from ferdelance.schemas.context import SchedulerContext, SchedulableJob
from ferdelance.schemas.models import GenericModel
from ferdelance.schemas.plans.plan import GenericPlan, Plan, PlanResult

import pandas as pd


class FusionPlan(GenericPlan):
    def __init__(self, name: str, fusion_plan: FusionPlan | None = None, random_seed: Any | None = None) -> None:
        super().__init__(name, random_seed)

        self.fusion_plan: FusionPlan | None = fusion_plan

    def get_fusion_plan(self) -> FusionPlan:
        if not self.fusion_plan:
            raise ValueError("No FusionPlan assigned")

        return self.fusion_plan

    def params(self) -> dict[str, Any]:
        return super().params()

    def build(self) -> Plan:
        """Converts the GenericPlan instance to a Plan exchange object.

        Returns:
            Plan:
                Object that can be sent to a server or a client in JSON format.
        """
        if self.fusion_plan is not None:
            return Plan(
                name=self.name,
                params=self.params(),
                plan=self.fusion_plan.build(),
            )
        return Plan(
            name=self.name,
            params=self.params(),
        )

    @abstractmethod
    def get_jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        raise NotImplementedError()

    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> PlanResult:
        return self.get_fusion_plan().run(df, local_model, working_folder, artifact_id)
