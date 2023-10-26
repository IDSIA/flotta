from __future__ import annotations
from typing import Any

from abc import abstractmethod, ABC
from itertools import pairwise

from ferdelance.core.distributions import Distribution
from ferdelance.core.entity import Entity, create_entities
from ferdelance.core.operations import Operation
from ferdelance.core.environment import Environment
from ferdelance.schemas.components import Component

from pydantic import BaseModel, root_validator


class SchedulableJob(BaseModel):
    id: int  # to keep track of the job's id
    worker: Component  # id of the worker
    iteration: int
    step: Step
    locks: list[int]  # list of jobs unlocked by this job

    @root_validator
    def create_subclass_entities(cls, values) -> dict[str, Any]:
        return create_entities(values)


class SchedulerContext(BaseModel):  # this is internal to the server
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
    def jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
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
            list[SchedulableJob]:
                A list of schedulable jobs without the bound to the next jobs.
        """
        raise NotImplementedError()

    @abstractmethod
    def bind(self, jobs0: list[SchedulableJob], jobs1: list[SchedulableJob]) -> None:
        """Assign the locks to the jobs0 list consideing the distribution used and the jobs1 list.

        Args:
            jobs0 (list[SchedulableJob]):
                Jobs created with the previous step
            jobs1 (list[SchedulableJob]):
                Jobs created with the next step

        Raises:
            NotImplementedError:
                If this method is not implemented.

        Returns:
            list[SchedulableJob]:
                A list of schedulabe jobs taht can be used to start the computations.
        """
        raise NotImplementedError()


class BlockStep(Step):
    operation: Operation
    distribution: Distribution | None = None
    inputs: list[str] = list()
    outputs: list[str] = list()
    iteration: int = 1

    def step(self, env: Environment) -> Environment:
        env = self.operation.exec(env)

        if self.distribution:
            self.distribution.distribute(env)

        return env

    def bind(self, jobs0: list[SchedulableJob], jobs1: list[SchedulableJob]) -> None:
        if self.distribution:
            jobs_id0 = [j.id for j in jobs0]
            jobs_id1 = [j.id for j in jobs1]

            locks = self.distribution.bind(jobs_id0, jobs_id1)

            for job, lock in zip(jobs0, locks):
                job.locks += lock


class Initialize(BlockStep):
    """Initial step performend by an initiator."""

    def __init__(
        self,
        operation: Operation,
        distribution: Distribution | None,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
    ) -> None:
        super(Initialize, self).__init__(
            operation=operation,
            distribution=distribution,
            inputs=inputs,
            outputs=outputs,
            iteration=iteration,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        return [
            SchedulableJob(
                id=context.get_id(),
                worker=context.initiator,
                iteration=context.iteration,
                locks=[],
                step=self,
            )
        ]


class Parallel(BlockStep):
    """Jobs are executed in parallel."""

    def __init__(
        self,
        operation: Operation,
        distribution: Distribution | None,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
    ) -> None:
        super(Parallel, self).__init__(
            operation=operation,
            distribution=distribution,
            inputs=inputs,
            outputs=outputs,
            iteration=iteration,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        return [
            SchedulableJob(
                id=context.get_id(),
                worker=worker,
                iteration=context.iteration,
                locks=[],
                step=self,
            )
            for worker in context.workers
        ]


class Sequential(BlockStep):
    """Jobs are executed in sequential order"""

    def __init__(
        self,
        operation: Operation,
        distribution: Distribution | None,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
    ) -> None:
        super(Sequential, self).__init__(
            operation=operation,
            distribution=distribution,
            inputs=inputs,
            outputs=outputs,
            iteration=iteration,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        job_list: list[SchedulableJob] = []

        for worker in context.workers:
            job_list.append(
                SchedulableJob(
                    id=context.get_id(),
                    worker=worker,
                    iteration=context.iteration,
                    locks=[],
                    step=self,
                )
            )

        for curr, next in pairwise(job_list):
            curr.locks.append(next.id)

        return job_list


class Finalize(BlockStep):
    def __init__(
        self,
        operation: Operation,
        distribution: Distribution | None = None,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
    ) -> None:
        super(Finalize, self).__init__(
            operation=operation,
            distribution=distribution,
            inputs=inputs,
            outputs=outputs,
            iteration=iteration,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        return [
            SchedulableJob(
                id=context.get_id(),
                worker=context.initiator,
                iteration=context.iteration,
                locks=[],
                step=self,
            )
        ]


class Iterate(Step):
    iterations: int
    steps: list[BlockStep]

    def jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        job_list = []

        for it in range(self.iterations):
            for step in self.steps:
                context.iteration = it
                job_list += step.jobs(context)

        return job_list


SchedulableJob.update_forward_refs()
