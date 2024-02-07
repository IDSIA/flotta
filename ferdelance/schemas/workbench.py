from ferdelance.schemas.datasources import DataSource
from ferdelance.schemas.client import ClientDetails

from pydantic import BaseModel

from datetime import datetime


class WorkbenchJoinRequest(BaseModel):
    """Data sent by the workbench to join the server."""

    id: str
    public_key: str
    version: str

    name: str = ""

    checksum: str
    signature: str


class WorkbenchJoinResponse(BaseModel):
    component_id: str


class WorkbenchProjectToken(BaseModel):
    token: str


class WorkbenchClientList(BaseModel):
    clients: list[ClientDetails]


class WorkbenchDataSourceIdList(BaseModel):
    datasources: list[DataSource]


class WorkbenchArtifact(BaseModel):
    artifact_id: str


class WorkbenchResource(BaseModel):
    resource_id: str
    producer_id: str
    creation_time: datetime | None = None
