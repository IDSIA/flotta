from __future__ import annotations
from typing import Any

from abc import abstractmethod, ABC
from itertools import pairwise

from ferdelance.schemas.plans.distributions import Distribution
from ferdelance.schemas.plans.operations import Operation
from ferdelance.schemas.components import Component

from pydantic import BaseModel


class SchedulableJob(BaseModel):
    id: int  # to keep track of the job's id
    worker: Component  # id of the worker
    iteration: int
    step: GenericStep
    locks: list[int]  # list of jobs unlocked by this job


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


class Step(BaseModel):
    name: str
    params: dict[str, Any]


class GenericStep(ABC):
    def __init__(self, name: str, iteration: int = 1) -> None:
        super().__init__()

        self.name: str = name
        self.iteration: int = iteration

    def params(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "iteration": self.iteration,
        }

    def build(self) -> Step:
        return Step(
            name=self.name,
            params=self.params(),
        )

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


class BlockStep(GenericStep):
    def __init__(
        self,
        name: str,
        # TODO: add here extraction and transformation queries?
        operation: Operation,
        distribution: Distribution | None = None,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
    ) -> None:
        super().__init__(name, iteration)

        self.operation: Operation = operation
        self.distribution: Distribution | None = distribution

        self.inputs: list[str] = inputs
        self.outputs: list[str] = outputs

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "operation": self.operation.build(),
            "distribution": self.distribution.build() if self.distribution else None,
            "inputs": self.inputs,
            "outputs": self.outputs,
        }

    def step(self, env: dict[str, Any]) -> dict[str, Any]:
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
        distribution: Distribution,
        outputs: list[str] = list(),
        iteration: int = 1,
    ) -> None:
        super().__init__(Initialize.__name__, operation, distribution, list(), outputs, iteration)

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
        distribution: Distribution,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
    ) -> None:
        super().__init__(Parallel.__name__, operation, distribution, inputs, outputs, iteration)

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
        distribution: Distribution,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
    ) -> None:
        super().__init__(Sequential.__name__, operation, distribution, inputs, outputs, iteration)

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
        super().__init__(Finalize.__name__, operation, distribution, inputs, outputs, iteration)

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


class Iterate(GenericStep):
    def __init__(
        self,
        iterations: int,
        steps: list[BlockStep],
    ) -> None:
        super().__init__(Iterate.__name__, 0)
        self.iterations: int = iterations
        self.steps: list[BlockStep] = steps

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "iterations": self.iterations,
            "steps": [step.build() for step in self.steps],  # TODO: check if this makes sense
        }

    def jobs(self, context: SchedulerContext) -> list[SchedulableJob]:
        job_list = []

        for it in range(self.iterations):
            for step in self.steps:
                context.iteration = it
                job_list += step.jobs(context)

        return job_list
