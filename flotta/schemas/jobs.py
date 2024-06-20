from flotta.shared.status import JobStatus

from datetime import datetime
from pathlib import Path
from pydantic import BaseModel


class Job(BaseModel):
    id: str
    artifact_id: str
    component_id: str
    path: Path
    status: JobStatus
    creation_time: datetime
    execution_time: datetime | None
    termination_time: datetime | None
    iteration: int


class JobLock(BaseModel):
    id: int
    job_id: str
    next_id: str
    locked: bool
