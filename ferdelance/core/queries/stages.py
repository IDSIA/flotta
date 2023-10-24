from ferdelance.core.entity import Entity

from ferdelance.core.queries.transformers import QueryTransformer
from ferdelance.core.queries.features import QueryFeature

from pydantic import validator


class QueryStage(Entity):
    """A stage is a single transformation applied to a list of features."""

    features: list[QueryFeature]  # list of available features (after the transformation below)
    transformer: QueryTransformer | None = None  # transformation to apply

    _features: dict[str, QueryFeature] = dict()

    @validator("features")
    def features_dict(cls, values):
        values["_features"] = dict()

        for f in values["features"]:
            if isinstance(f, QueryFeature):
                key = f.name
            else:
                key = f["name"]
            values["_features"][key] = f

    def __getitem__(self, key: str | QueryFeature) -> QueryFeature:
        if isinstance(key, QueryFeature):
            key = key.name

        if key not in self._features:
            raise ValueError(f"feature {key} not found")

        return self._features[key]
