from __future__ import annotations
from typing import Any, Sequence
from abc import abstractmethod, ABC

from ferdelance.core.entity import Entity, create_entities
from ferdelance.schemas.components import Component

from pydantic import BaseModel, root_validator


class SchedulerJob(BaseModel):
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


SchedulerJob.update_forward_refs()
