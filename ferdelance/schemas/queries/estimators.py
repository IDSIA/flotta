from __future__ import annotations
from typing import Any

from ferdelance.schemas.queries.features import QueryFeature

from pydantic import BaseModel


class QueryEstimator(BaseModel):
    """Query estimator to apply to the features from the workbench."""

    features_in: list[QueryFeature]
    name: str
    parameters: dict[str, Any]

    def params(self) -> dict[str, Any]:
        return {
            "features_in": self.features_in,
        } | self.parameters

    def __eq__(self, other: QueryEstimator) -> bool:
        if not isinstance(other, QueryEstimator):
            return False

        return self.features_in == other.features_in and self.name == other.name

    def __hash__(self) -> int:
        return hash((self.features_in, self.name))

    def __str__(self) -> str:
        return f"{self.name}({self.features_in})"
