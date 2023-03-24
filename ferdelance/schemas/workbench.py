from ferdelance.schemas.datasources import DataSource
from ferdelance.schemas.client import ClientDetails

from pydantic import BaseModel


class WorkbenchJoinRequest(BaseModel):
    """Data sent by the workbench to join the server."""

    public_key: str


class WorkbenchJoinData(BaseModel):
    """Data sent by the server to a workbench after a successful join."""

    id: str
    token: str
    public_key: str


class WorkbenchProjectToken(BaseModel):
    token: str


class WorkbenchClientList(BaseModel):
    clients: list[ClientDetails]


class WorkbenchDataSourceIdList(BaseModel):
    datasources: list[DataSource]


class WorkbenchArtifact(BaseModel):
    artifact_id: str
