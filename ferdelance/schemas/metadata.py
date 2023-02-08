from __future__ import annotations

from ferdelance.schemas.datasources import BaseDataSource, BaseFeature

from pydantic import BaseModel


class Metadata(BaseModel):
    """Information on data stored in the client."""

    datasources: list[MetaDataSource]


class MetaDataSource(BaseDataSource):
    """Information on data sources stored in the client."""

    datasource_id: str | None
    datasource_hash: str

    tokens: list[str]

    removed: bool = False
    features: list[MetaFeature]


class MetaFeature(BaseFeature):
    """Information on features stored in the client."""

    datasource_hash: str  # identifier for datasource client-side
    removed: bool = False
