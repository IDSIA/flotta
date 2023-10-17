from typing import Any

from ferdelance.logging import get_logger
from ferdelance.schemas.models import GenericModel
from ferdelance.schemas.plans.fusion.core import FusionPlan, SchedulerContext, SchedulableJob, PlanResult
from ferdelance.schemas.plans.local import LocalPlan

import pandas as pd


LOGGER = get_logger(__name__)


class SequencePlan(FusionPlan):
    def __init__(
        self,
        local_plan: LocalPlan,
        random_seed: float | None = None,
    ) -> None:
        super().__init__(SequencePlan.__name__, None, random_seed)

        self.local_plan: LocalPlan = local_plan

    def params(self) -> dict[str, Any]:
        return super().params()

    def get_jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        jobs = []

        if context.current_iteration == 0:
            jobs.append(
                SchedulableJob(
                    id=0,
                    artifact_id=context.artifact_id,
                    worker=context.initiator,
                    iteration=context.current_iteration,
                    counter=0,
                    unlocks=[1],
                    work_type="init",
                )
            )

        job_id = 0
        for worker in context.workers:
            job_id += 1
            jobs.append(
                SchedulableJob(
                    id=job_id,
                    artifact_id=context.artifact_id,
                    worker=worker,
                    iteration=context.current_iteration,
                    counter=1,
                    unlocks=[job_id + 1],
                    work_type="local",
                )
            )

        jobs[-1].unlocks = list()  # reset unlocks parameter for last job

        return jobs

    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> PlanResult:
        if self.local_plan is None:
            raise ValueError("No local plan defined!")

        return self.local_plan.run(df, local_model, working_folder, artifact_id)
