from __future__ import annotations
from typing import Sequence
from itertools import pairwise

from ferdelance.core.distributions import Distribution, DirectToNext
from ferdelance.core.environment import Environment
from ferdelance.core.interfaces import Step, SchedulerJob, SchedulerContext
from ferdelance.core.operations import Operation


class BaseStep(Step):
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

    def jobs(self, context: SchedulerContext) -> Sequence[SchedulerJob]:
        return [
            SchedulerJob(
                id=context.get_id(),
                worker=context.initiator,
                iteration=context.iteration,
                locks=[],
                step=self,
            )
        ]


class Initialize(BaseStep):
    """Initial step performed by an initiator."""

    def __init__(
        self,
        operation: Operation,
        distribution: Distribution | None = None,
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


class Parallel(BaseStep):
    """Jobs are executed in parallel."""

    def __init__(
        self,
        operation: Operation,
        distribution: Distribution | None = None,
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


class Sequential(BaseStep):
    """Jobs are executed in sequential order"""

    init_operation: Operation
    final_operation: Operation

    def __init__(
        self,
        init_operation: Operation,
        operation: Operation,
        final_operation: Operation,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
        **data,
    ) -> None:
        super(Sequential, self).__init__(
            init_operation=init_operation,  # type: ignore
            operation=operation,
            final_operation=final_operation,  # type: ignore
            inputs=inputs,
            outputs=outputs,
            iteration=iteration,
            **data,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        job_list: list[SchedulerJob] = []

        # initialization job
        job_list.append(
            SchedulerJob(
                id=context.get_id(),
                worker=context.initiator,
                iteration=context.iteration,
                locks=[],
                step=BaseStep(
                    iteration=self.iteration,
                    operation=self.init_operation,
                    distribution=DirectToNext(next=context.workers[0]),
                ),
            )
        )

        # worker jobs
        for worker in context.workers:
            job_list.append(
                SchedulerJob(
                    id=context.get_id(),
                    worker=worker,
                    iteration=context.iteration,
                    locks=[],
                    step=BaseStep(
                        iteration=self.iteration,
                        operation=self.operation,
                        distribution=DirectToNext(next=context.workers[0]),
                    ),
                )
            )

        # finalization job
        job_list.append(
            SchedulerJob(
                id=context.get_id(),
                worker=context.initiator,
                iteration=context.iteration,
                locks=[],
                step=BaseStep(
                    iteration=self.iteration,
                    operation=self.final_operation,
                    distribution=None,
                ),
            )
        )

        # link all jobs together
        for curr, next in pairwise(job_list):
            curr.locks.append(next.id)

        return job_list


class Finalize(BaseStep):
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
    steps: list[BaseStep]

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        job_list = []

        for it in range(self.iterations):
            for step in self.steps:
                context.iteration = it
                job_list += step.jobs(context)

        return job_list
