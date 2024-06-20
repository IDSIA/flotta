from __future__ import annotations
from typing import Sequence
from itertools import pairwise

from flotta.core.distributions import Distribution, DirectToNext
from flotta.core.interfaces import SchedulerJob, SchedulerContext, BaseStep
from flotta.core.operations import Operation

from pydantic import SerializeAsAny


class Initialize(BaseStep):
    """Initial step performed by an initiator."""

    def __init__(
        self,
        operation: Operation,
        distribution: SerializeAsAny[Distribution | None] = None,
        iteration: int = 1,
        **data,
    ) -> None:
        super(Initialize, self).__init__(
            operation=operation,
            distribution=distribution,
            iteration=iteration,
            **data,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        return [
            SchedulerJob(
                id=context.get_id(),
                worker=context.initiator,
                iteration=context.iteration,
                step=self,
            )
        ]


class Parallel(BaseStep):
    """Jobs are executed in parallel."""

    def __init__(
        self,
        operation: Operation,
        distribution: Distribution | None = None,
        iteration: int = 1,
        **data,
    ) -> None:
        super(Parallel, self).__init__(
            operation=operation,
            distribution=distribution,
            iteration=iteration,
            **data,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        return [
            SchedulerJob(
                id=context.get_id(),
                worker=worker,
                iteration=context.iteration,
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
        iteration: int = 1,
        **data,
    ) -> None:
        super(Sequential, self).__init__(
            init_operation=init_operation,  # type: ignore
            operation=operation,
            final_operation=final_operation,  # type: ignore
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


class RoundRobin(BaseStep):
    """Circular distribution of jobs between workers."""

    def __init__(
        self,
        operation: Operation,
        iteration: int = 1,
        **data,
    ) -> None:
        super(RoundRobin, self).__init__(
            operation=operation,
            iteration=iteration,
            distribution=None,
            **data,
        )

    def jobs(self, context: SchedulerContext) -> Sequence[SchedulerJob]:
        job_list = []

        n_workers = len(context.workers)

        # TODO: this is a draft, check if it is correctly implemented
        # FIXME: keep track of the produced resource! It need to pass through ALL workers!

        for _ in range(n_workers):
            for i in range(n_workers):
                start_worker = context.workers[i]
                end_worker = context.workers[(i + 1) % n_workers]

                job_list.append(
                    SchedulerJob(
                        id=context.get_id(),
                        worker=start_worker,
                        iteration=context.iteration,
                        step=BaseStep(
                            iteration=self.iteration,
                            operation=self.operation,
                            distribution=DirectToNext(next=end_worker),
                        ),
                    )
                )

        return job_list


class Finalize(BaseStep):
    """Completion job done by the initiator."""

    def __init__(
        self,
        operation: SerializeAsAny[Operation],
        distribution: SerializeAsAny[Distribution | None] = None,
        iteration: int = 1,
        **data,
    ) -> None:
        super(Finalize, self).__init__(
            operation=operation,
            distribution=distribution,
            iteration=iteration,
            **data,
        )

    def jobs(self, context: SchedulerContext) -> list[SchedulerJob]:
        return [
            SchedulerJob(
                id=context.get_id(),
                worker=context.initiator,
                iteration=context.iteration,
                step=self,
            )
        ]
