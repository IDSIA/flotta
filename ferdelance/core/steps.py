from __future__ import annotations

from itertools import pairwise

from ferdelance.core.distributions import Distribution
from ferdelance.core.operations import Operation
from ferdelance.core.environment import Environment

from ferdelance.core.interfaces import Step, SchedulerJob, SchedulerContext


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

    def bind(self, jobs0: list[SchedulerJob], jobs1: list[SchedulerJob]) -> None:
        if self.distribution:
            jobs_id0 = [j.id for j in jobs0]
            jobs_id1 = [j.id for j in jobs1]

            locks = self.distribution.bind(jobs_id0, jobs_id1)

            for job, lock in zip(jobs0, locks):
                job.locks += lock


class Initialize(BlockStep):
    """Initial step performed by an initiator."""

    def __init__(
        self,
        operation: Operation,
        distribution: Distribution | None,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
        **data,
    ) -> None:
        super(Initialize, self).__init__(
            operation=operation,
            distribution=distribution,
            inputs=inputs,
            outputs=outputs,
            iteration=iteration,
            **data,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        return [
            SchedulerJob(
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
        **data,
    ) -> None:
        super(Parallel, self).__init__(
            operation=operation,
            distribution=distribution,
            inputs=inputs,
            outputs=outputs,
            iteration=iteration,
            **data,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        return [
            SchedulerJob(
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
        **data,
    ) -> None:
        super(Sequential, self).__init__(
            operation=operation,
            distribution=distribution,
            inputs=inputs,
            outputs=outputs,
            iteration=iteration,
            **data,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        job_list: list[SchedulerJob] = []

        for worker in context.workers:
            job_list.append(
                SchedulerJob(
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
        **data,
    ) -> None:
        super(Finalize, self).__init__(
            operation=operation,
            distribution=distribution,
            inputs=inputs,
            outputs=outputs,
            iteration=iteration,
            **data,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        return [
            SchedulerJob(
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

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        job_list = []

        for it in range(self.iterations):
            for step in self.steps:
                context.iteration = it
                job_list += step.jobs(context)

        return job_list
