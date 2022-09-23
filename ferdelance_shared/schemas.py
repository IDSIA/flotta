from pydantic import BaseModel
from datetime import datetime


class ClientJoinRequest(BaseModel):
    """Data sent by the client to join the server."""
    system: str
    mac_address: str
    node: str

    public_key: str  # b64encoded bytes
    version: str


class ClientJoinData(BaseModel):
    """Data sent by the server to the client after a successful join."""
    id: str
    token: str
    public_key: str


class UpdateData(BaseModel):
    """Basic update response from the server with the next action to do."""
    action: str


class UpdateToken(UpdateData):
    """The client has a new token to use."""
    token: str


class UpdateClientApp(UpdateData):
    """Data for the client on the new app to download."""
    checksum: str
    name: str
    version: str


class UpdateExecute(UpdateData):
    """Task that the client has to execute next."""
    client_task_id: str


class UpdateNothing(UpdateData):
    """Nothing else to do."""
    pass


class DownloadApp(BaseModel):
    """Details from the client to the app to download"""
    name: str
    version: str


class Feature(BaseModel):
    """Description of a feature for the server."""
    feature_id: str
    datasource_id: str

    name: str
    dtype: str | None

    created_at: datetime

    v_mean: float | None
    v_std: float | None
    v_min: float | None
    v_p25: float | None
    v_p50: float | None
    v_p75: float | None
    v_max: float | None
    v_miss: float | None


class DataSourceDetails(BaseModel):
    """Basic information on a data source on the client. This can be sent to the workbench."""
    datasource_id: str

    name: str | None

    created_at: datetime

    n_records: int | None
    n_features: int | None

    features: list[Feature]


class DataSource(DataSourceDetails):
    """Extra information from the client for the metadata"""
    client_id: str

    removed: bool = False

    type: str | None


class Metadata(BaseModel):
    """Update information from the client for the server."""
    datasources: list[DataSource]


class QueryFeature(BaseModel):
    """Query feature to use in a query from the workbench."""
    feature_id: str
    datasource_id: str


class QueryFilter(BaseModel):
    """Query filter to apply to the feature from the workbench."""
    feature: QueryFeature
    operation: str
    parameter: str


class QueryTransformer(BaseModel):
    """Query transformation to apply to the feature from the workbench."""
    feature: QueryFeature
    name: str
    parameters: str


class Query(BaseModel):
    """Query to apply to the selected data from the workbench."""
    datasources_id: str
    features: list[QueryFeature]
    filters: list[QueryFilter]
    transformers: list[QueryTransformer]


class Model(BaseModel):
    """Model selected int the workbench."""
    name: str
    model: str | None


class Strategy(BaseModel):
    """Strategy selected int the workbench."""
    strategy: str


class BaseArtifact(BaseModel):
    """Basic structure for artifact"""
    artifact_id: str | None


class Artifact(BaseArtifact):
    """Artifact created in the workbench."""
    queries: list[Query]
    model: Model
    strategy: Strategy


class ArtifactStatus(BaseArtifact):
    """Details on the artifact."""
    status: str | None


class ArtifactTask(BaseArtifact):
    """Task sent to the client for dataset preparation."""
    client_task_id: str
    queries: list[Query]
    model: Model


class ClientDetails(BaseModel):
    client_id: str
    created_at: datetime
    version: str
