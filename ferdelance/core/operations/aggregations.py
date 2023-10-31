from __future__ import annotations

from ferdelance.core.environment import Environment
from ferdelance.core.model_operations import ModelOperation


class Aggregation(ModelOperation):
    def exec(self, env: Environment) -> Environment:
        models = env["models"]

        base = models[0]

        for model in models[1:]:
            base = self.model.aggregate(base, model)

        env["aggregated_model"] = base
        return env


Aggregation.update_forward_refs()
