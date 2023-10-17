from typing import Any

from ferdelance.logging import get_logger
from ferdelance.schemas.models import GenericModel
from ferdelance.schemas.plans.fusion.core import FusionPlan, SchedulerContext, SchedulableJob, PlanResult

import pandas as pd

LOGGER = get_logger(__name__)


class IterativePlan(FusionPlan):
    def __init__(
        self,
        local_plan: FusionPlan,
        iterations: int = -1,
        random_seed: float | None = None,
    ) -> None:
        super().__init__(IterativePlan.__name__, local_plan, random_seed)

        self.iterations: int = iterations

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "iterations": self.iterations,
        }

    def get_jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        if context.current_iteration == self.iterations:
            LOGGER.info("Reached max iterations, no mor jobs to execute")
            return list()

        return self.get_fusion_plan().get_jobs(context)

    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> PlanResult:
        if self.fusion_plan is None:
            raise ValueError("No local plan defined!")

        return self.fusion_plan.run(df, local_model, working_folder, artifact_id)

    async def post_aggregation_hook(self, context: SchedulerContext) -> None:
        context.schedule_next_iteration = context.current_iteration < self.iterations - 1
