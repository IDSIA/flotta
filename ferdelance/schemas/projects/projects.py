from __future__ import annotations

from .datasources import BaseDataSource, Feature
from ferdelance.schemas.artifacts.queries import Query

from pydantic import BaseModel
from datetime import datetime


class BaseProject(BaseModel):
    """This comes from the database."""

    project_id: str
    token: str
    name: str

    creation_time: datetime

    valid: bool
    active: bool


class Project(BaseProject):
    """This is what goes to the workbench."""

    n_datasources: int
    n_clients: int

    data: AggregatedDataSource

    def describe(self) -> str:
        # TODO
        raise NotImplementedError()


class AggregatedDataSource(BaseDataSource):
    queries: list[Query] = list()

    features: list[AggregatedFeature] = list()

    def describe(self) -> str:
        # TODO
        raise NotImplementedError()

    def add_query(self, query: Query) -> None:
        self.queries.append(query)

    def __add__(self, other: Query) -> AggregatedDataSource:
        if isinstance(other, Query):
            self.add_query(other)
            return self

        raise ValueError("Cannot add something that is not a Query")


class AggregatedFeature(Feature):
    n_datasources: int
