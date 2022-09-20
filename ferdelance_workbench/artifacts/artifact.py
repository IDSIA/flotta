from __future__ import annotations

from typing import Any

from .model import Model, model_from_json
from .query import Query, query_from_json
from .strategy import Strategy, strategy_from_json


class Artifact:

    def __init__(self, query: Query | None = None, model: Model | None = None, strategy: Strategy | None = None, artifact_id: str | None = None, status: str | None = None) -> None:
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


def artifact_from_json(artifact_id: str, data: dict[str, Any]) -> Artifact:
    a = Artifact(artifact_id=artifact_id, status=data.get('status', None))

    a.query = query_from_json(data['query'])
    a.model = model_from_json(data['model'])
    a.strategy = strategy_from_json(data['strategy'])

    return a
