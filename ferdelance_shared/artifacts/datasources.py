from pydantic import BaseModel
from .features import Feature, MetaFeature


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
