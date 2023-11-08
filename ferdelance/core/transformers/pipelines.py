from typing import Any, Sequence

from ferdelance.core.environment import Environment
from ferdelance.core.transformers.core import QueryTransformer


class FederatedPipeline(QueryTransformer):
    """A pipeline that can be used to group sequence of Transformers.
    The stages of the pipeline will be applied in sequence to the input data.

    A pipeline can also be nested inside another pipeline.
    """

    stages: Sequence[QueryTransformer] = list()

    def transform(self, env: Environment) -> tuple[Environment, Any]:
        trs = list()
        for stage in self.stages:
            env, tr = stage.transform(env)
            trs.append(tr)

        return env, trs

    def aggregate(self, env: Environment) -> Environment:
        # TODO
        return super().aggregate(env)
