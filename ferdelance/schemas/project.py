from __future__ import annotations
from pydantic import BaseModel
from datetime import datetime


# --- project ---


class ProjectBase(BaseModel):
    project_id: str
    token: str
    name: str
    creation_time: datetime


class Project(ProjectBase):
    valid: bool
    active: bool

    descr: str
    n_datasources: int
    n_clients: int

    data: AggregatedDataSource

    def describe(self) -> str:
        # TODO
        raise NotImplementedError()


class ProjectCreate(ProjectBase):
    token: str


# --- datasource ---


class DataSourceBase(BaseModel):
    datasource_id: str
    datasource_hash: str

    name: str

    creation_time: datetime
    update_time: datetime
    removed: datetime

    n_records: int
    n_features: int


class DataSource(DataSourceBase):
    projects: list[Project] = list()
    features: list[Feature] = list()


class AggregatedDataSource(DataSourceBase):
    features: list[AggregatedFeature] = list()

    def describe(self) -> str:
        # TODO
        raise NotImplementedError()


class DataSourceCreate(DataSourceBase):
    pass


# --- feature ---


class FeatureBase(BaseModel):
    feature_id: str

    name: str
    dtype: str

    v_mean: float
    v_std: float
    v_min: float
    v_p25: float
    v_p50: float
    v_p75: float
    v_max: float
    v_miss: float

    n_cats: int


class Feature(FeatureBase):
    datasource: DataSource


class AggregatedFeature(FeatureBase):
    n_datasources: int


class FeatureCreate(FeatureBase):
    creation_time: datetime
    update_time: datetime
    removed: bool
