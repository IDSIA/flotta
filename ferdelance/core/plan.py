from __future__ import annotations
from abc import ABC
from typing import Any, Sequence

from ferdelance.core.entity import Entity
from ferdelance.core.steps import Step, SchedulableJob, SchedulerContext

from itertools import pairwise

from pydantic import BaseModel


class Plan(ABC, Entity):
    """This is a plan that can produce jobs given a list of steps."""

    steps: Sequence[Step]
    random_seed: Any = None

    def jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        jobs = []

        jobs0 = self.steps[0].jobs(context)

        for step0, step1 in pairwise(self.steps):
            jobs1 = step1.jobs(context)

            step0.bind(jobs0, jobs1)

            jobs += jobs0
            jobs0 = jobs1

        jobs += jobs0

        return jobs


class Artifact(Plan):
    """Standard implementation of a generic plan."""

    id: str
    project_id: str


class ArtifactStatus(BaseModel):
    """Details on the artifact status."""

    id: str
    status: str | None
    resource: str | None = None
    iteration: int = 0
