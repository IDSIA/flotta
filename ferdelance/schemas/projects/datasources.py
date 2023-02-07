from __future__ import annotations

from ferdelance.schemas.artifacts.queries import (
    Query,
    QueryFeature,
)

from pydantic import BaseModel
from datetime import datetime


class BaseDataSource(BaseModel):
    """Common information to all data sources."""

    datasource_id: str

    name: str

    removed: datetime

    n_records: int
    n_features: int


class DataSource(BaseDataSource):
    # TODO: this need to keep track of everything through a query node

    client_id: str

    datasource_id: str
    datasource_hash: str

    features: list[Feature] = list()
    features_by_id: dict[str, Feature] = dict()
    features_by_name: dict[str, Feature] = dict()

    def __init__(self, **data):
        super().__init__(**data)

        self.features_by_id: dict[str, Feature] = {f.feature_id: f for f in self.features}
        self.features_by_name: dict[str, Feature] = {f.name: f for f in self.features}

    def all_features(self):
        return Query(
            datasource_id=self.datasource_id, datasource_name=self.name, features=[f.qf() for f in self.features]
        )

    def __eq__(self, other: DataSource) -> bool:
        if not isinstance(other, DataSource):
            return False

        return self.datasource_id == other.datasource_id

    def __hash__(self) -> int:
        return hash(self.datasource_id)

    def info(self) -> str:
        lines: list[str] = list()

        lines.append(f"{self.datasource_id} {self.name} ({self.n_features}x{self.n_records})")

        for df in self.features:
            mean = 0.0 if df.v_mean is None else df.v_mean
            lines.append(f"- {df.dtype:8} {df.name:32} {mean:.2}")

        return f"\n".join(lines)

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
        return super().__str__() + f"client_id={self.client_id} features=[{self.features}]"


class BaseFeature(BaseModel):
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

    n_cats: int | None


class Feature(BaseFeature):
    """Common information to all features."""

    feature_id: str
    datasource_id: str
    datasource_name: str

    def qf(self) -> QueryFeature:
        return QueryFeature(
            feature_id=self.feature_id,
            datasource_id=self.datasource_id,
            feature_name=self.name,
            datasource_name=self.datasource_name,
            dtype=self.dtype,
        )

    def info(self) -> str:
        lines: list[str] = [
            f"{self.name}",
            f"Data type:            {self.dtype}",
        ]

        if self.dtype != "object":
            lines += [
                f"Value min:            {self.v_min:.2f}",
                f"Value max:            {self.v_max:.2f}",
                f"Mean:                 {self.v_mean:.2f}",
                f"Std deviation:        {self.v_std:.2f}",
                f"Value 25° percentile: {self.v_p25:.2f}",
                f"Value 50° percentile: {self.v_p50:.2f}",
                f"Value 75° percentile: {self.v_p75:.2f}",
                f"Missing value:        {self.v_miss:.2f}",
            ]

        return "\n".join(lines)
