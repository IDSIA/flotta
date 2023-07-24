from typing import Any

from pydantic import BaseModel

from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models.metrics import Metrics


class TaskArguments(BaseModel):
    private_key_location: str
    server_url: str
    server_public_key: str
    token: str
    workdir: str
    datasources: list[dict[str, Any]]
    job_id: str
    artifact_id: str


class TaskExecutionParameters(BaseModel):
    artifact: Artifact
    job_id: str
    datasource_hashes: list[str]


class TaskAggregationParameters(BaseModel):
    artifact: Artifact
    job_id: str
    result_ids: list[str]


class ExecutionResult(BaseModel):
    job_id: str
    path: str
    metrics: list[Metrics]
    is_model: bool = False
    is_estimate: bool = False
