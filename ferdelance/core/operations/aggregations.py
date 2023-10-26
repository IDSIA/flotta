from ferdelance.core.environment import Environment
from ferdelance.core.models.core import Model
from ferdelance.core.operations.core import Operation


class Aggregation(Operation):
    model: Model

    def exec(self, env: Environment) -> Environment:
        models = env["models"]

        base = models[0]

        for model in models[1:]:
            base = self.model.aggregate(base, model)

        env["aggregated_model"] = base
        return env
