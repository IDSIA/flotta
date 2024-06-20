from ferdelance.core.entity import Entity
from ferdelance.core.environment import Environment
from ferdelance.core.queries.features import QueryFeature
from ferdelance.core.transformers import QueryTransformer


class QueryStage(Entity):
    """A stage is a single transformation applied to a list of features."""

    index: int = -1

    features: list[QueryFeature]  # list of available features (after a transformation is applied)
    transformer: QueryTransformer | None = None  # transformation to apply

    def apply(self, env: Environment) -> Environment:
        if self.transformer is not None:
            env, tr = self.transformer.transform(env)
            env[f"stage_{self.index}"] = tr

        return env

    def __call__(self, env: Environment) -> Environment:
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

        for f in self.features:
            if f.name == key:
                return f

        raise ValueError(f"feature {key} not found")
