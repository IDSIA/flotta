from pydantic import BaseModel

from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.components import Component


class JobFromContext(BaseModel):
    id: int  # to keep track of the job's id
    worker: Component  # id of the worker
    artifact: Artifact
    iteration: int  # current iteration (0-based)
    counter: int
    unlocks: list[int]
    work_type: str


class TaskContext(BaseModel):
    artifact: Artifact

    initiator: Component  # component_id of the initiator
    workers: list[Component]  # list of component_ids of the involved clients

    job_total: int = 0
    job_completed: int = 0
    job_failed: int = 0

    aggregations: int = 0
    aggregations_failed: int = 0

    completed_threshold: float = 1.0

    allow_errors: bool = False

    current_iteration: int = -1
    next_iteration: int = -1

    schedule_next_iteration: bool = False

    def completed(self) -> bool:
        all_jobs_completed = self.job_completed == self.job_total
        above_threshold_completed = self.job_completed / self.job_total > self.completed_threshold

        return all_jobs_completed or above_threshold_completed

    def has_failed(self) -> bool:
        if self.allow_errors:
            return True
        return self.job_failed > 0

    def has_aggregations_failed(self) -> bool:
        if self.aggregations_failed > 0:
            return True
        return False
