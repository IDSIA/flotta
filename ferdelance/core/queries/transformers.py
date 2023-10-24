from __future__ import annotations
from typing import Any

from ferdelance.core.entity import Entity
from ferdelance.core.queries.features import QueryFeature


class QueryTransformer(Entity):
    """Query transformation to apply to the features from the workbench."""

    features_in: list[QueryFeature]
    features_out: list[QueryFeature]
    name: str
    parameters: dict[str, Any]

    def __eq__(self, other: QueryTransformer) -> bool:
        if not isinstance(other, QueryTransformer):
            return False

        return (
            self.features_in == other.features_in
            and self.features_out == other.features_out
            and self.name == other.name
        )

    def __hash__(self) -> int:
        return hash((self.features_in, self.features_out, self.name))

    def __str__(self) -> str:
        return f"{self.name}({self.features_in} -> {self.features_out})"
