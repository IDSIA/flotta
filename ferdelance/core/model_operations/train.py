from __future__ import annotations

from ferdelance.logging import get_logger
from ferdelance.core.environment import Environment
from ferdelance.core.model_operations import ModelOperation


LOGGER = get_logger(__name__)


class Train(ModelOperation):
    """Execution plan that train a model over all the available data.
    No evaluation step is performed.
    """

    def exec(self, env: Environment) -> Environment:
        env = self.query.apply(env)

        if env.X_tr is None or env.y_tr is None:
            raise ValueError("Cannot train a model without X_tr and y_tr")

        # model training
        env["local_model"] = self.model.train(env.X_tr.values, env.y_tr)

        LOGGER.info(f"artifact={env.artifact_id}: local model train completed")

        return env
