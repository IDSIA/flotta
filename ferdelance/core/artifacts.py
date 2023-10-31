from __future__ import annotations
from typing import Any, Sequence
from itertools import pairwise

from ferdelance.core.entity import Entity
from ferdelance.core.interfaces import Step, SchedulerJob, SchedulerContext

from pydantic import BaseModel


class Artifact(Entity):
    """This is a plan that can produce jobs given a list of steps."""

    id: str
    project_id: str
    steps: Sequence[Step]
    random_state: Any = None

    def jobs(self, context: SchedulerContext) -> Sequence[SchedulerJob]:
        jobs = []

        jobs0 = self.steps[0].jobs(context)

        for step0, step1 in pairwise(self.steps):
            jobs1 = step1.jobs(context)

            step0.bind(jobs0, jobs1)

            jobs += jobs0
            jobs0 = jobs1

        jobs += jobs0

        return jobs


class ArtifactStatus(BaseModel):
    """Details on the artifact status."""

    id: str
    status: str | None
    resource: str | None = None
    iteration: int = 0
