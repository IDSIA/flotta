from __future__ import annotations
from abc import ABC
from typing import Any

from ferdelance.schemas.plans.steps import Step, SchedulableJob, SchedulerContext

# from pydantic import BaseModel

from itertools import pairwise


# class Plan(BaseModel):
#     """This is the JSON that will be exchanged within an Artifact."""

#     name: str
#     params: dict[str, Any]
#     steps: list[Step]


class Plan:
    """This is the JSON that will be exchanged within an Artifact."""

    def __init__(
        self,
        name: str,
        params: dict[str, Any],
        steps: list[Step],
    ) -> None:
        self.params = params
        self.name = name
        self.steps = steps


class GenericPlan(ABC):
    def __init__(self, name: str, *steps: Step, random_seed: Any = None) -> None:
        self.name: str = name
        self.random_seed: Any = random_seed

        self.steps: list[Step] = list(steps)

    def params(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "random_seed": self.random_seed,
        }

    def build(self) -> Plan:
        """Converts the GenericPlan instance to a Plan exchange object.

        Returns:
            Plan:
                Object that can be sent to a server or a client in JSON format.
        """
        return Plan(
            name=self.name,
            params=self.params(),
            steps=self.steps,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        jobs = []

        for step0, step1 in pairwise(self.steps):
            jobs0 = step0.jobs(context)
            jobs1 = step1.jobs(context)

            step0.bind(jobs0, jobs1)

            jobs += jobs0
            jobs += jobs1

        return jobs


# TODO: this should be the new artifact
class SimplePlan(GenericPlan):
    def __init__(self, *steps: Step, random_seed: Any = None) -> None:
        super().__init__(SimplePlan.__name__, *steps, random_seed)
