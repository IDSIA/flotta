from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Model(BaseModel):
    model_id: str
    creation_time: Optional[datetime]
    path: str
    aggregated: Optional[bool]
    artifact_id: str
    client_id: str


class DataSource(BaseModel):
    datasource_id: str

    name: str

    creation_time: datetime
    update_time: datetime
    removed: bool

    n_records: int | None
    n_features: int | None

    client_id: str


class Project(BaseModel):
    project_id: str
    name: str
    creation_time: datetime
    token: str
    valid: bool
    active: bool
