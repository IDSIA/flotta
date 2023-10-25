from typing import Any
from ferdelance.core.entity import Entity

from ferdelance.core.queries.features import QueryFeature
from ferdelance.core.transformers import QueryTransformer

from pydantic import validator, PrivateAttr


class QueryStage(Entity):
    """A stage is a single transformation applied to a list of features."""

    index: int = -1

    features: list[QueryFeature]  # list of available features (after a transformation is applied)
    transformer: QueryTransformer  # transformation to apply

    _features: dict[str, QueryFeature] = PrivateAttr(dict())

    @validator("features")
    def features_dict(cls, values):
        values["_features"] = dict()

        for f in values["features"]:
            if isinstance(f, QueryFeature):
                key = f.name
            else:
                key = f["name"]
            values["_features"][key] = f

    def apply(self, env: dict[str, Any]) -> dict[str, Any]:
        X_tr = env.get("X_tr", None)
        y_tr = env.get("y_tr", None)
        X_ts = env.get("X_ts", None)
        y_ts = env.get("y_tr", None)

        X_tr, y_tr, X_ts, y_ts, tr = self.transformer.transform(X_tr, y_tr, X_ts, y_ts)

        env["X_tr"] = X_tr
        env["y_tr"] = y_tr
        env["X_ts"] = X_ts
        env["y_tr"] = y_ts

        env[f"stage_{self.index}"] = tr

        return env

    def __call__(self, env: dict[str, Any]) -> dict[str, Any]:
        return self.apply(env)

    def __getitem__(self, key: str | QueryFeature) -> QueryFeature:
        """_summary_

        Args:
            key (str | QueryFeature): _description_

        Raises:
            ValueError: _description_

        Returns:
            QueryFeature: _description_
        """
        if isinstance(key, QueryFeature):
            key = key.name

        if key not in self._features:
            raise ValueError(f"feature {key} not found")

        return self._features[key]
