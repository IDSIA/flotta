from __future__ import annotations

from typing import Any

from .model import Model
from .query import Query
from .strategy import Strategy


class Artifact:

    def __init__(self, query: Query, model: Model, strategy: Strategy, artifact_id: str | None = None, status: str | None = None) -> None:
        self.artifact_id: str | None = artifact_id
        self.status: str | None = status

        self.query: Query = query
        self.model: Model = model
        self.strategy: Strategy = strategy

    def update(self, data: dict[str, Any]) -> None:
        self.status = data.get('status', None)

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return f'Artifact#{self.artifact_id}: {self.status}'
