from pydantic import BaseModel
from ..models import Model


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
    datasource_name: str


class MetaFeature(BaseFeature):
    """Information on features stored in the client."""
    removed: bool = False


class BaseDataSource(BaseModel):
    """Common information to all data sources."""
    n_records: int | None
    n_features: int | None

    name: str


class DataSource(BaseDataSource):
    """Information for the workbench."""
    client_id: str
    datasource_id: str

    features: list[Feature]


class MetaDataSource(BaseDataSource):
    """Information on data source stored in the client."""
    removed: bool = False

    features: list[MetaFeature]


class Metadata(BaseModel):
    """Information on data stored in the client."""
    datasources: list[MetaDataSource]


class QueryFeature(BaseModel):
    """Query feature to use in a query from the workbench."""
    feature_id: str
    datasource_id: str
    feature_name: str
    datasource_name: str


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
    datasource_id: str
    datasource_name: str
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


class BaseArtifact(BaseModel):
    """Basic structure for artifact"""
    artifact_id: str | None


class Artifact(BaseArtifact):
    """Artifact created in the workbench."""
    dataset: Dataset
    model: Model


class ArtifactStatus(BaseArtifact):
    """Details on the artifact."""
    status: str | None
