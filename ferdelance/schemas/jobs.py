from datetime import datetime

from pydantic import BaseModel


class Job(BaseModel):
    job_id: int
    artifact_id: str
    component_id: str
    status: str
    creation_time: datetime
    execution_time: datetime | None
    termination_time: datetime | None
    is_model: bool = False
    is_estimation: bool = False
    is_aggregation: bool = False
    celery_id: str | None = None
