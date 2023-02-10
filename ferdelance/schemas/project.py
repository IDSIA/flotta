from __future__ import annotations

from ferdelance.schemas.datasources import AggregatedDataSource

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
