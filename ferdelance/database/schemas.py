from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class BaseComponent(BaseModel):
    type_name: str

    public_key: str

    active: bool
    left: bool


class Component(BaseComponent):
    component_id: str


class Client(BaseComponent):
    client_id: str

    version: str

    machine_system: str
    machine_mac_address: str
    machine_node: str

    blacklisted: bool
    ip_address: str


class Token(BaseModel):
    token_id: int
    component_id: str
    token: str
    creation_time: datetime
    expiration_time: float
    valid: bool


class Job(BaseModel):
    job_id: int
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


class Event(BaseModel):
    component_id: str
    event_id: int
    event_time: datetime
    event: str
