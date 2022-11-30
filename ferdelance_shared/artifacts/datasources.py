from __future__ import annotations
from pydantic import BaseModel
from .queries import (
    Query,
    QueryFeature,
    Feature,
    MetaFeature,
)


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
    features_by_id: dict[str, Feature] = dict()
    features_by_name: dict[str, Feature] = dict()

    def __init__(self, **data):
        super().__init__(**data)

        self.features_by_id: dict[str, Feature] = {f.feature_id: f for f in self.features}
        self.features_by_name: dict[str, Feature] = {f.name: f for f in self.features}

    def all_features(self):
        return Query(
            datasource_id=self.datasource_id,
            datasource_name=self.name,
            features=[f.qf() for f in self.features]
        )

    def __eq__(self, other: DataSource) -> bool:
        if not isinstance(other, DataSource):
            return False

        return self.datasource_id == other.datasource_id

    def __hash__(self) -> int:
        return hash(self.datasource_id)

    def info(self) -> str:
        lines: list[str] = list()

        lines.append(f'{self.datasource_id} {self.name} ({self.n_features}x{self.n_records})')

        for df in self.features:
            mean = .0 if df.v_mean is None else df.v_mean
            lines.append(f'- {df.dtype:8} {df.name:32} {mean:.2}')

        return f'\n'.join(lines)

    def features_dict(self) -> dict[str, QueryFeature]:
        return {f.name: f.qf() for f in self.features}

    def __getitem__(self, key: str | QueryFeature) -> Feature:

        # TODO: add support for list of keys in / list of features out

        if isinstance(key, str):
            f = self.features_by_id.get(key, None) or self.features_by_name.get(key, None)
            if f:
                return f

        if isinstance(key, QueryFeature):
            f = self.features_by_id.get(key.feature_id, None)
            if f:
                return f

        raise ValueError(f'Feature "{str(key)}" not found in this datasource')

    def __str__(self) -> str:
        return super().__str__() + f'client_id={self.client_id} features=[{self.features}]'


class MetaDataSource(BaseDataSource):
    """Information on data source stored in the client."""
    removed: bool = False

    features: list[MetaFeature]


class Metadata(BaseModel):
    """Information on data stored in the client."""
    datasources: list[MetaDataSource]
