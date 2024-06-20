from ferdelance.schemas.components import Component

from pydantic import BaseModel

# TODO: this has been/need to be moved to fdl.core


class SchedulerContext(BaseModel):  # this is internal to the server
    artifact_id: str

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
