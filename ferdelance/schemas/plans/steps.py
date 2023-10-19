from typing import Any

from abc import abstractmethod, ABC

from ferdelance.schemas.plans.distributions import Distribution
from ferdelance.schemas.plans.operations import Operation


class SchedulableJob:
    ...


class BaseStep(ABC):
    def __init__(self, iteration: int = 1) -> None:
        super().__init__()
        self.iteration: int = iteration

    @abstractmethod
    def jobs(self) -> list[SchedulableJob]:
        # TODO: maybe use "topology of network" as input?
        raise NotImplementedError()


class Step(BaseStep):
    def __init__(
        self,
        # TODO: add here extraction and transformation queries?
        operation: Operation,
        distribution: Distribution | None = None,
        inputs: list[str] = list(),
        outputs: list[str] = list(),
        iteration: int = 1,
    ) -> None:
        super().__init__(iteration)

        self.operation: Operation = operation
        self.distriute: Distribution | None = distribution

        self.inputs: list[str] = inputs
        self.outputs: list[str] = outputs

    def step(self, env: dict[str, Any]) -> dict[str, Any]:
        env = self.operation.exec(env)

        if self.distriute:
            self.distriute.distribute(env)

        return env


class Initialize(Step):
    def jobs(self) -> list[SchedulableJob]:
        # TODO
        return super().jobs()


class Parallel(Step):
    def jobs(self) -> list[SchedulableJob]:
        # TODO
        return super().jobs()


class Sequential(Step):
    def jobs(self) -> list[SchedulableJob]:
        # TODO
        return super().jobs()


class Finalize(Step):
    def jobs(self) -> list[SchedulableJob]:
        # TODO
        return super().jobs()


class Iterate(BaseStep):
    def __init__(
        self,
        iterations: int,
        steps: list[Step],
    ) -> None:
        super().__init__(0)
        self.iterations: int = iterations
        self.steps: list[Step] = steps

    def jobs(self) -> list[SchedulableJob]:
        job_list = []
        for it in range(self.iterations):
            for step in self.steps:
                step.iteration = it
                job_list += step.jobs()
        return job_list
