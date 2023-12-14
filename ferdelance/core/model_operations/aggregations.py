from __future__ import annotations

from ferdelance.core.environment import Environment
from ferdelance.core.model_operations import ModelOperation


class Aggregation(ModelOperation):
    def exec(self, env: Environment) -> Environment:
        resource_ids = env.list_resource_ids()

        base = env[resource_ids[0]]["model"]

        for resource_id in resource_ids[1:]:
            model = env[resource_id]["model"]

            base = self.model.aggregate(base, model)

        env["model"] = base

        return env


Aggregation.update_forward_refs()
