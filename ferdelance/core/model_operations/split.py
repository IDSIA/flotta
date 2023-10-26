from ferdelance.logging import get_logger
from ferdelance.core.environment import Environment
from ferdelance.core.model_operations.train import ModelOperation, Train
from ferdelance.core.metrics import Metrics


LOGGER = get_logger(__name__)


class TrainTest(ModelOperation):
    """Execution plan that train a model on a percentage of available data and test it on the remaining part."""

    source: str = "test"
    trainer: Train

    def exec(self, env: Environment) -> Environment:
        artifact_id: str = env.artifact_id

        env = self.query.apply(env)

        if env.X_tr is None or env.y_tr is None:
            raise ValueError("Cannot train without train data")

        # model training
        env, model = self.model.train(env.X_tr, env.y_tr)
        env["local_model"] = model

        LOGGER.info(f"artifact={artifact_id}: train done")

        # model testing
        if env.X_ts is None or env.y_ts is None:
            raise ValueError("Cannot eval without test data")

        metrics_list: list[Metrics] = list()

        x_ts = env.X_ts.values
        y_ts = env.y_ts

        metrics = model.eval(x_ts, y_ts)
        metrics.source = self.source
        metrics.artifact_id = artifact_id

        metrics_list.append(metrics)

        env["metrics_list"] = metrics_list

        return env
