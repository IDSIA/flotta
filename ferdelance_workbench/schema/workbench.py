from pydantic import BaseModel

from datetime import datetime


class ClientDetails(BaseModel):
    client_id: str
    created_at: datetime
    version: str


class DataSource(BaseModel):
    datasource_id: int
    name: str | None
    type: str | None
    created_at: datetime
    n_records: int | None
    n_features: int | None

    client_id: str


class Feature(BaseModel):
    feature_id: int
    datasource_id: int

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
    datasource: DataSource
    features: list[Feature]
