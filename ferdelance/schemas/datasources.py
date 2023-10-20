from __future__ import annotations
from enum import Enum, auto

from ferdelance.schemas.queries import (
    Query,
    QueryFeature,
    QueryStage,
)
from pydantic import BaseModel
from hashlib import sha256

import pandas as pd


class DataType(Enum):
    """Mapping of pandas data types.

    - BOOLEAN: `bool`
    - CATEGORICAL: `category`
    - DATETIME: `datetime64` or `timedelta[ns]`
    - NUMERIC: `int64` or `float64`
    - STRING: `object`
    """

    BOOLEAN = auto()
    CATEGORICAL = auto()
    DATETIME = auto()
    NUMERIC = auto()
    STRING = auto()


class BaseFeature(BaseModel):
    name: str

    dtype: str | None = None

    v_mean: float | None = None
    v_std: float | None = None
    v_min: float | None = None
    v_p25: float | None = None
    v_p50: float | None = None
    v_p75: float | None = None
    v_max: float | None = None
    v_miss: float | None = None

    n_cats: int | None = None


class Feature(BaseFeature):
    """Common information to all features."""

    def qf(self) -> QueryFeature:
        return QueryFeature(
            name=self.name,
            dtype=self.dtype,
        )

    def info(self) -> str:
        lines: list[str] = [
            f"{self.name}",
            f"Data type:             {self.dtype}",
        ]

        if self.dtype != "object":
            lines += [
                f"Value min:             {self.v_min:.2f}",
                f"Value max:             {self.v_max:.2f}",
                f"Mean:                  {self.v_mean:.2f}",
                f"Std deviation:         {self.v_std:.2f}",
                f"Value 25th percentile: {self.v_p25:.2f}",
                f"Value 50th percentile: {self.v_p50:.2f}",
                f"Value 75th percentile: {self.v_p75:.2f}",
                f"Missing value:         {self.v_miss:.2f}",
            ]

        return "\n".join(lines)


class AggregatedFeature(Feature):
    n_datasources: int = 1

    # TODO: aggregated features should have distributions, not punctual values!

    @staticmethod
    def aggregate(features: list[Feature]) -> AggregatedFeature:
        name = features[0].name
        dtype = features[0].dtype

        df = pd.DataFrame([f.dict() for f in features])

        if dtype == DataType.NUMERIC.name:
            return AggregatedFeature(
                name=name,
                dtype=dtype,
                n_datasources=len(features),
                v_mean=df["v_mean"].mean(),
                v_std=df["v_std"].mean(),
                v_min=df["v_min"].mean(),
                v_p25=df["v_p25"].mean(),
                v_p50=df["v_p50"].mean(),
                v_p75=df["v_p75"].mean(),
                v_max=df["v_max"].mean(),
                v_miss=df["v_miss"].mean(),
            )

        return AggregatedFeature(
            name=name,
            dtype=dtype,
            n_datasources=len(features),
            v_miss=df["v_miss"].mean(),
        )


class BaseDataSource(BaseModel):
    """Common information to all data sources."""

    name: str

    n_records: int
    n_features: int


class DataSource(BaseDataSource):
    # TODO: should we remove this in favor of AggregatedDataSource?
    # TODO: this need to keep track of everything through a query node

    component_id: str

    id: str
    hash: str

    features: list[Feature] = list()
    _features_by_name: dict[str, Feature] = dict()

    def __init__(self, **data):
        super().__init__(**data)

        for f in self.features:
            self._features_by_name[f.name] = f

    def __eq__(self, other: DataSource) -> bool:
        if not isinstance(other, DataSource):
            return False

        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    def info(self) -> str:
        lines: list[str] = list()

        lines.append(f"{self.id} {self.name} ({self.n_features}x{self.n_records})")

        for df in self.features:
            mean = 0.0 if df.v_mean is None else df.v_mean
            lines.append(f"- {df.dtype:8} {df.name:32} {mean:.2}")

        return "\n".join(lines)

    def features_dict(self) -> dict[str, QueryFeature]:
        return {f.name: f.qf() for f in self.features}

    def __getitem__(self, key: str | QueryFeature) -> Feature:
        # TODO: add support for list of keys in / list of features out

        if isinstance(key, str):
            f = self._features_by_name.get(key, None)
            if f:
                return f

        if isinstance(key, QueryFeature):
            f = self._features_by_name.get(key.name, None)
            if f:
                return f

        raise ValueError(f'Feature "{str(key)}" not found in this datasource')

    def __str__(self) -> str:
        return super().__str__() + f"component={self.component_id} features=[{self.features}]"

    def extract(self) -> Query:
        """Proceeds on extracting all the features and creating a transformation
        query from this data source.

        :return:
            A new query object with the first stage initialized from the
            available features.
        """

        return Query(
            stages=[
                QueryStage(
                    features=[f.qf() for f in self.features],
                )
            ]
        )


class AggregatedDataSource(BaseDataSource):
    hash: str

    # list of initial features
    features: list[AggregatedFeature] = list()
    _features_by_name: dict[str, AggregatedFeature] = dict()

    n_clients: int = 0
    n_datasources: int = 0

    def __init__(self, **data):
        super().__init__(**data)

        for f in self.features:
            self._features_by_name[f.name] = f

    @staticmethod
    def aggregate(datasources: list[DataSource], encoding: str = "utf8") -> AggregatedDataSource:
        n_records = 0
        clients = set()
        features: dict[tuple[str, str], list[Feature]] = dict()
        hashes = sha256()

        for ds in datasources:
            n_records += ds.n_records
            clients.add(ds.component_id)
            hashes.update(ds.hash.encode(encoding))

            for f in ds.features:
                if f.dtype is None:
                    continue
                key = (f.name, f.dtype)
                if key not in features:
                    features[key] = [f]
                else:
                    features[key].append(f)

        return AggregatedDataSource(
            name="",
            n_records=n_records,
            n_features=len(features),
            n_clients=len(clients),
            n_datasources=len(datasources),
            features=[AggregatedFeature.aggregate(f) for _, f in features.items()],
            hash=hashes.hexdigest(),
        )

    def info(self) -> str:
        lines: list[str] = list()

        lines.append(f"{self.name} ({self.n_features}x{self.n_records})")

        for df in self.features:
            mean = 0.0 if df.v_mean is None else df.v_mean
            lines.append(f"- {df.dtype:8} {df.name:32} {mean:.2}")

        return "\n".join(lines)

    def extract(self) -> Query:
        """Proceeds on extracting all the features and creating a transformation
        query from this data source.

        :return:
            A new query object with the first stage initialized from the
            available features.
        """

        return Query(
            stages=[
                QueryStage(
                    features=[f.qf() for f in self.features],
                )
            ]
        )

    def __getitem__(self, key: str | QueryFeature) -> AggregatedFeature:
        if isinstance(key, QueryFeature):
            key = key.name

        if key not in self._features_by_name:
            raise ValueError(f"feature {key} is not part of the Data Source")

        return self._features_by_name[key]

    def __eq__(self, other: AggregatedDataSource) -> bool:
        if not isinstance(other, AggregatedDataSource):
            return False

        return self.hash == other.hash

    def __str__(self) -> str:
        return super().__str__() + f"datasource_hash={self.hash} features=[{self.features}]"
