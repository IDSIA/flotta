from typing import Any

from ferdelance.schemas.queries.transformers import QueryTransformer
from ferdelance.schemas.queries.features import QueryFeature

from pydantic import BaseModel


class QueryStage(BaseModel):
    """A stage is a single transformation applied to a list of features."""

    features: list[QueryFeature]  # list of available features (after the transformation below)
    transformer: QueryTransformer | None = None  # transformation to apply

    _features: dict[str, QueryFeature] = dict()

    def __init__(
        self,
        features: list[QueryFeature],
        transformer: QueryTransformer | None = None,
        **data: Any,
    ) -> None:
        super().__init__(features=features, transformer=transformer, **data)

        for f in self.features:
            self._features[f.name] = f

    def __getitem__(self, key: str | QueryFeature) -> QueryFeature:
        if isinstance(key, QueryFeature):
            key = key.name

        if key not in self._features:
            raise ValueError(f"feature {key} not found")

        return self._features[key]
