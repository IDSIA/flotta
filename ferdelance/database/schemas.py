from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Component(BaseModel):
    component_id: str

    version: str
    public_key: str

    machine_system: str
    machine_mac_address: str
    machine_node: str

    type: str

    active: bool
    blacklisted: bool
    left: bool
    ip_address: str


class User(BaseModel):
    user_id: str
    public_key: str
    active: bool
    left: bool


class Job(BaseModel):
    job_id: str
    artifact_id: str
    client_id: str
    status: str
    creation_time: datetime
    execution_time: Optional[datetime]
    termination_time: Optional[datetime]


class Artifact(BaseModel):
    artifact_id: str
    creation_time: datetime
    path: str
    status: str


class Model(BaseModel):
    model_id: str
    creation_time: datetime
    path: str
    aggregated: bool
    artifact_id: str
    client_id: str
