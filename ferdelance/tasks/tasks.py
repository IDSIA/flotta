from ferdelance.core.interfaces import SchedulerJob

from pydantic import BaseModel


class TaskRequest(BaseModel):
    """Schema to request the current context to use."""

    artifact_id: str
    job_id: str  # job to fetch


class Task(BaseModel):
    artifact_id: str
    job_id: str  # current job

    iteration: int

    job: SchedulerJob

    next_url: str  # if next is client, then url is the server itself
    next_public_key: str  # always the final receiver


class TaskDone(BaseModel):
    artifact_id: str
    job_id: str


class TaskError(BaseModel):
    job_id: str = ""
    message: str = ""
    stack_trace: str = ""
