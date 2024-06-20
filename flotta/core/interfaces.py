from __future__ import annotations
from typing import Any, Sequence
from abc import abstractmethod, ABC
from itertools import pairwise

from ferdelance.core.entity import Entity, create_entities
from ferdelance.core.environment import Environment
from ferdelance.core.distributions import Distribution
from ferdelance.core.operations import Operation
from ferdelance.schemas.components import Component

from pydantic import BaseModel, SerializeAsAny, model_validator


class SchedulerJob(BaseModel):
    id: int  # to keep track of the job's id
    worker: Component  # id of the worker
    iteration: int
    step: SerializeAsAny[BaseStep | Iterate]
    locks: list[int] = list()  # list of jobs unlocked by this job

    resource_required: list[tuple[str, str]] = list()  # [(resource_id, producer_id)] for previous node
    resource_produced: list[tuple[str, str]] = list()  # [(resource_id, consumer_id)] for next node

    @model_validator(mode="before")
    def create_subclass_entities(cls, values) -> dict[str, Any]:
        return create_entities(values)


class SchedulerContext(BaseModel):
    """This is representation of the worker nodes internally available to a scheduler."""

    artifact_id: str

    initiator: Component  # component_id of the initiator
    workers: list[Component]  # list of component_ids of the involved clients

    iteration: int = 0

    current_id: int = 0

    def get_id(self) -> int:
        i = self.current_id
        self.current_id += 1
        return i


class Step(ABC, Entity):
    iteration: int = 1

    @abstractmethod
    def jobs(self, context: SchedulerContext) -> Sequence[SchedulerJob]:
        """Returns a list of all the jobs that this step will create. These jobs
        are not connected to the next. To create a bound, use the bind() method
        between two sets of jobs.

        Args:
            context (SchedulerContext):
                The topology to use.

        Raises:
            NotImplementedError:
                When this method is not implemented.

        Returns:
            list[SchedulerJob]:
                A list of jobs that can be scheduled without the bound to the next jobs.
        """
        raise NotImplementedError()

    @abstractmethod
    def bind(self, jobs0: Sequence[SchedulerJob], jobs1: Sequence[SchedulerJob]) -> None:
        """Assign the locks to the jobs0 list considering the distribution used and the jobs1 list.

        Args:
            jobs0 (list[SchedulerJob]):
                Jobs created with the previous step
            jobs1 (list[SchedulerJob]):
                Jobs created with the next step

        Raises:
            NotImplementedError:
                If this method is not implemented.

        Returns:
            list[SchedulerJob]:
                A list of jobs that can be scheduled that can be used to start the computations.
        """
        raise NotImplementedError()

    @abstractmethod
    def step(self, env: Environment) -> Environment:
        raise NotImplementedError()


class BaseStep(Step):
    operation: Operation
    distribution: Distribution | None = None
    iteration: int = 1

    def step(self, env: Environment) -> Environment:
        return self.operation.exec(env)

    def bind(self, jobs0: Sequence[SchedulerJob], jobs1: Sequence[SchedulerJob]) -> None:
        if self.distribution:
            jobs_id0 = [j.id for j in jobs0]
            jobs_id1 = [j.id for j in jobs1]

            locks = self.distribution.bind_locks(jobs_id0, jobs_id1)

            for job, lock in zip(jobs0, locks):
                job.locks += lock

    def jobs(self, context: SchedulerContext) -> Sequence[SchedulerJob]:
        return [
            SchedulerJob(
                id=context.get_id(),
                worker=context.initiator,
                iteration=context.iteration,
                step=self,
            )
        ]


class Iterate(Step):
    """Repeat the step multiple times."""

    iterations: int
    steps: SerializeAsAny[list[BaseStep]]

    def step(self, env: Environment) -> Environment:
        raise ValueError("Iterate is a meta-step and should not be executed!")

    def bind(self, jobs0: Sequence[SchedulerJob], jobs1: Sequence[SchedulerJob]) -> None:
        raise ValueError("Iterate is a meta-step and does not have a bind method!")

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        job_list: Sequence[SchedulerJob] = []

        last_job = None
        for it in range(self.iterations):
            # create jobs for current iteration
            it_jobs: Sequence[SchedulerJob] = []

            context.iteration = it
            jobs0: Sequence[SchedulerJob] = self.steps[0].jobs(context)

            for step0, step1 in pairwise(self.steps):
                jobs1 = step1.jobs(context)

                step0.bind(jobs0, jobs1)

                it_jobs += jobs0
                jobs0 = jobs1

            it_jobs += jobs0

            if last_job is not None:
                # add locks to last job of previous iteration
                last_job.locks += [job.id for job in it_jobs]

            job_list += it_jobs
            last_job = job_list[-1]

        return job_list


SchedulerJob.model_rebuild()
