from __future__ import annotations
from typing import Any

from ferdelance.core.queries.features import QueryFeature

from pydantic import BaseModel


class QueryTransformer(BaseModel):
    """Query transformation to apply to the features from the workbench."""

    features_in: list[QueryFeature]
    features_out: list[QueryFeature]
    name: str
    parameters: dict[str, Any]

    def params(self) -> dict[str, Any]:
        return {
            "features_in": self.features_in,
            "features_out": self.features_out,
        } | self.parameters

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
