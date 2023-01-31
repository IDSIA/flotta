from pydantic import BaseModel
from datetime import datetime
from ferdelance.shared.artifacts import DataSource


class WorkbenchJoinRequest(BaseModel):
    """Data sent by the workbench to join the server."""

    public_key: str


class WorkbenchJoinData(BaseModel):
    """Data sent by the server to a workbench after a successful join."""

    id: str
    token: str
    public_key: str


class WorkbenchClientList(BaseModel):
    client_ids: list[str]


class WorkbenchDataSourceIdList(BaseModel):
    datasource_ids: list[str]


class WorkbenchDataSourceList(BaseModel):
    datasources: list[DataSource]


class WorkbenchProject(BaseModel):
    project_id: str
    name: str
    creation_time: str
    token: str
    valid: bool
    active: bool


class WorkbenchProjectDescription(BaseModel):

    project_id: str
    name: str
    creation_time: str
    token: str
    valid: bool
    active: bool

    n_datasources: int
    avg_n_features: float


class AggregatedDataSource(BaseModel):
    """Acts like a single datasource, describes features with distributions instead of punctual values."""

    datasources: list[DataSource]
