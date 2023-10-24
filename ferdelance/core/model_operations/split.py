from typing import Any

from ferdelance.logging import get_logger
from ferdelance.core.model_operations.train import ModelOperation, Train
from ferdelance.core.metrics import Metrics

from sklearn.model_selection import train_test_split

import pandas as pd


LOGGER = get_logger(__name__)


class TrainTestSplit(ModelOperation):
    """Execution plan that train a model on a percentage of available data and test it on the remaining part."""

    test_percentage: float = 0.0
    stratified: bool = True
    source: str = "test"
    random_seed: Any = None

    trainer: Train

    def run(self, env: dict[str, Any]) -> dict[str, Any]:
        df: pd.DataFrame = env["df"]
        artifact_id: str = env["artifact_id"]

        test_p = self.test_percentage

        self.validate_input(df)

        df_tr: pd.DataFrame = df
        df_ts: pd.DataFrame | None = None

        if test_p > 0.0:
            if self.stratified:
                df_tr, df_ts = train_test_split(
                    df,
                    test_size=test_p,
                    stratify=df[self.label],
                    random_state=self.random_seed,
                )

            else:
                df_tr, df_ts = train_test_split(
                    df,
                    test_size=test_p,
                    random_state=self.random_seed,
                )

        # model training
        env["df"] = df_tr
        env = self.trainer.run(env)
        local_model = env["local_model"]

        LOGGER.info(f"artifact={artifact_id}: train done")

        # model testing
        metrics_list: list[Metrics] = list()
        if df_ts is not None:
            x_ts = df_ts.drop(self.label, axis=1).values
            y_ts = df_ts[self.label].values

            metrics = local_model.eval(x_ts, y_ts)
            metrics.source = self.source
            metrics.artifact_id = artifact_id

            metrics_list.append(metrics)

        env["metrics_list"] = metrics_list

        return env
