from __future__ import annotations

from ferdelance.core.environment import Environment
from ferdelance.core.model_operations import ModelOperation


class Aggregation(ModelOperation):
    def exec(self, env: Environment) -> Environment:
        resource_ids = list(env.stored_resources.keys())

        base = env.stored_resources[resource_ids[0]].get()

        for resource_id in resource_ids[1:]:
            model = env.stored_resources[resource_id].get()

            base = self.model.aggregate(base, model)

        env.set_product(base)

        return env


Aggregation.update_forward_refs()
