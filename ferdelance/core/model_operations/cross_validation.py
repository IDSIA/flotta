from ferdelance.core.environment import Environment
from ferdelance.core.metrics import Metrics
from ferdelance.core.model_operations.core import ModelOperation
from ferdelance.logging import get_logger

from sklearn.model_selection import StratifiedKFold, KFold


LOGGER = get_logger(__name__)


class LocalCrossValidation(ModelOperation):
    """Execution plan that duplicates the input data and apply two different plans to them."""

    folds: int = 10
    stratified: bool = True
    shuffle: bool = True
    source: str = "test"

    def exec(self, env: Environment) -> Environment:
        if self.stratified:
            kf = StratifiedKFold(self.folds, shuffle=True, random_state=self.random_state)
        else:
            kf = KFold(self.folds, shuffle=self.shuffle, random_state=self.random_state)

        x = env.X_tr
        y = env.y_tr

        if x is None:
            raise ValueError("X_tr is required")

        if y is None:
            raise ValueError("y_tr is required")

        metrics_list: list[Metrics] = list()

        for fold, (tr, ts) in kf.split(x, y):
            x_tr = x.iloc[tr].copy()
            y_tr = y.iloc[tr].copy()
            x_ts = x.iloc[ts].copy()
            y_ts = y.iloc[ts].copy()

            model = self.model.train(x_tr, y_tr)

            metrics = model.eval(x_ts, y_ts)
            metrics.source = f"{self.source}_{fold}"
            metrics.artifact_id = env.artifact_id

            metrics_list.append(metrics)

        env["metrics_list"] = metrics_list

        return env
