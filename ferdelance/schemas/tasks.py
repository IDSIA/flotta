from typing import Any

from ferdelance.core.artifacts import Artifact

from pydantic import BaseModel


class TaskParametersRequest(BaseModel):
    """Schema to request the current context to use."""

    artifact_id: str
    job_id: str  # current job


class TaskParameters(BaseModel):
    """Schema containing the current context to use in a job."""

    job_id: str  # current job

    artifact_id: str
    iteration: int

    data: dict[str, Any] = dict()

    next_url: str  # if next is client, then url is the server itself
    next_public_key: str  # always the final receiver


class Task(BaseModel):
    artifact: Artifact
    params: TaskParameters


class TaskDone(BaseModel):
    artifact_id: str
    job_id: str
    resource_id: str


class TaskError(BaseModel):
    job_id: str = ""
    message: str = ""
    stack_trace: str = ""
