from __future__ import annotations
from abc import ABC
from typing import Any
from ferdelance.schemas.plans.entity import Entity, create_entities

from ferdelance.schemas.plans.steps import Step, SchedulableJob, SchedulerContext

from itertools import pairwise


class Plan(ABC, Entity):
    """This is a plan that can produce jobs given a list of steps."""

    steps: list[Step]
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

    # def __init__(self, steps: list[Step], id: str, project_id: str, random_seed: Any = None, **kwargs) -> None:
    #     super(Artifact, self).__init__(steps=steps, id=id, project_id=project_id, random_seed=random_seed)
