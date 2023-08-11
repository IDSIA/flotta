from datetime import datetime

from pydantic import BaseModel


class Job(BaseModel):
    id: str
    artifact_id: str
    component_id: str
    status: str
    creation_time: datetime
    execution_time: datetime | None
    termination_time: datetime | None
    is_model: bool = False
    is_estimation: bool = False
    is_aggregation: bool = False
    iteration: int
