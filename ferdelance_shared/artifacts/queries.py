from pydantic import BaseModel


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
