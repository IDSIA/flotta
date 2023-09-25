from ferdelance.schemas.datasources import DataSource
from ferdelance.schemas.client import ClientDetails

from pydantic import BaseModel


class WorkbenchJoinRequest(BaseModel):
    """Data sent by the workbench to join the server."""

    id: str
    public_key: str
    version: str

    name: str = ""

    checksum: str
    signature: str


class WorkbenchProjectToken(BaseModel):
    token: str


class WorkbenchClientList(BaseModel):
    clients: list[ClientDetails]


class WorkbenchDataSourceIdList(BaseModel):
    datasources: list[DataSource]


class WorkbenchArtifact(BaseModel):
    artifact_id: str


class WorkbenchArtifactPartial(WorkbenchArtifact):
    producer_id: str
    iteration: int
