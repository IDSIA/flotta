from typing import Any
from ferdelance.core.models.core import Model
from ferdelance.core.operations.core import Operation


class Aggregation(Operation):
    model: Model

    def exec(self, env: dict[str, Any]) -> dict[str, Any]:
        model = env["model"]
        model.aggregate(env)  # TODO: this does not make any sense...
        env["model"] = model
        return env
