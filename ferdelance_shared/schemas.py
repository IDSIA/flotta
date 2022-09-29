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


class ClientDetails(BaseModel):
    client_id: str
    created_at: datetime
    version: str


class ClientUpdate(BaseModel):
    action: str


class ClientUpdateTaskCompleted(ClientUpdate):
    client_task_id: str
    # TODO: consider return errors to workbench


class UpdateData(BaseModel):
    """Basic update response from the server with the next action to do."""
    action: str


class UpdateToken(UpdateData):
    """The client has a new token to use."""
    token: str

    def __str__(self) -> str:
        return super().__str__()


class UpdateClientApp(UpdateData):
    """Data for the client on the new app to download."""
    checksum: str
    name: str
    version: str

    def __str__(self) -> str:
        return f'{super().__str__()}, name={self.name}, version={self.version}'


class UpdateExecute(UpdateData):
    """Task that the client has to execute next."""
    job_id: str


class UpdateNothing(UpdateData):
    """Nothing else to do."""
    pass


class DownloadApp(BaseModel):
    """Details from the client to the app to download"""
    name: str
    version: str


class BaseFeature(BaseModel):
    """Common information to all features."""
    name: str
    dtype: str | None

    v_mean: float | None
    v_std: float | None
    v_min: float | None
    v_p25: float | None
    v_p50: float | None
    v_p75: float | None
    v_max: float | None
    v_miss: float | None


class Feature(BaseFeature):
    """Information for the workbench."""
    feature_id: str
    datasource_id: str


class MetaFeature(BaseFeature):
    """Information on features stored in the client."""
    removed: bool = False


class BaseDataSource(BaseModel):
    """Common information to all data sources."""
    n_records: int | None
    n_features: int | None


class DataSource(BaseDataSource):
    """Information for the workbench."""
    client_id: str
    datasource_id: str
    name: str

    features: list[Feature]


class MetaDataSource(BaseDataSource):
    """Information on data source stored in the client."""
    name: str | None
    removed: bool = False

    features: list[MetaFeature]


class Metadata(BaseModel):
    """Information on data stored in the client."""
    datasources: list[MetaDataSource]


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
    features: list[QueryFeature] = list()
    filters: list[QueryFilter] = list()
    transformers: list[QueryTransformer] = list()


class Dataset(BaseModel):
    """Query split the data in train/test/validation."""
    queries: list[Query]
    test_percentage: float = 0.0
    val_percentage: float = 0.0
    random_seed: float | None = None
    label: str | None = None


class Model(BaseModel):
    """Model selected int the workbench."""
    name: str
    model: str | None = None


class Strategy(BaseModel):
    """Strategy selected int the workbench."""
    strategy: str


class BaseArtifact(BaseModel):
    """Basic structure for artifact"""
    artifact_id: str | None


class Artifact(BaseArtifact):
    """Artifact created in the workbench."""
    dataset: Dataset
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
