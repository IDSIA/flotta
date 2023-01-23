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


class Component(BaseComponent):
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
    creation_time: Optional[datetime]
    path: str
    aggregated: Optional[bool]
    artifact_id: str
    client_id: str


class DataSource(BaseModel):
    """Table that collects the data source available on each client."""

    datasource_id: str

    name: str

    creation_time: datetime
    update_time: datetime
    removed: bool

    n_records: int
    n_features: int

    client_id: str
