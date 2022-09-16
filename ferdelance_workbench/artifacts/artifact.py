from __future__ import annotations

from .model import Model
from .query import Query
from .strategy import Strategy

from uuid import uuid4


class Artifact:

    def __init__(self, query: Query, model: Model, strategy: Strategy) -> None:
        self.artifact_id = str(uuid4())  # TODO: this should be set by the server
        self.query: Query = query
        self.model: Model = model
        self.strategy: Strategy = strategy

    def ret(self) -> Artifact:
        return self
