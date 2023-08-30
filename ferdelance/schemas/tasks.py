from typing import Any

from pydantic import BaseModel

from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models.metrics import Metrics


class TaskArguments(BaseModel):
    """Used to launch a new local task."""

    private_key: str
    server_url: str
    server_public_key: str
    token: str
    workdir: str
    datasources: list[dict[str, Any]]
    job_id: str
    artifact_id: str


class TaskParametersRequest(BaseModel):
    """Sent to a server's get_task_param request."""

    artifact_id: str
    job_id: str


class TaskParameters(BaseModel):
    """Returned from a server's get_task_params request."""

    artifact: Artifact
    job_id: str
    content_ids: list[str]


class ExecutionResult(BaseModel):
    job_id: str
    path: str
    metrics: list[Metrics]
    is_model: bool = False
    is_estimate: bool = False
