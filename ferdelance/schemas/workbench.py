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


# OLD SCHEMAS vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv


class WorkbenchDataSource(BaseModel):

    component_id: str
    datasource_id: str
    name: str
    n_features: int

    # features: list[Feature]
    # features_by_name: dict[str, Feature] = dict()


class WorkbenchProject(BaseModel):

    project_id: str
    name: str

    creation_time: str

    token: str

    valid: bool
    active: bool

    datasources: list[WorkbenchDataSource]


class WorkbenchProjectDescription(BaseModel):

    project_id: str
    name: str
    creation_time: str
    token: str
    valid: bool
    active: bool

    n_datasources: int
    avg_n_features: float
