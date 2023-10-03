from typing import Any

from ferdelance.logging import get_logger
from ferdelance.schemas.plans.local.core import LocalPlan, GenericModel, Metrics

from sklearn.model_selection import train_test_split

import pandas as pd


LOGGER = get_logger(__name__)


class TrainTestSplit(LocalPlan):
    """Execution plan that train a model on a percentage of available data and test it on the remaining part."""

    def __init__(
        self,
        label: str,
        local_plan: LocalPlan,
        test_percentage: float = 0.0,
        stratified: bool = True,
        source: str = "test",
        random_seed: Any = None,
    ) -> None:
        super().__init__(TrainTestSplit.__name__, label, random_seed, local_plan)
        self.test_percentage: float = test_percentage
        self.stratified: bool = stratified
        self.source: str = source

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "test_percentage": self.test_percentage,
            "stratified": self.stratified,
            "source": self.source,
        }

    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> list[Metrics]:
        test_p = self.test_percentage

        self.validate_input(df)

        if not self.local_plan:
            raise ValueError("No local_plan assigned")

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
        self.local_plan.run(
            df_tr,
            local_model,
            working_folder,
            artifact_id,
        )

        LOGGER.info(f"artifact={artifact_id}: saved model to {self.path_model}")

        # model testing
        metrics_list: list[Metrics] = list()
        if df_ts is not None:
            x_ts = df_ts.drop(self.label, axis=1).values
            y_ts = df_ts[self.label].values

            metrics = local_model.eval(x_ts, y_ts)
            metrics.source = self.source
            metrics.artifact_id = artifact_id

            metrics_list.append(metrics)

        return metrics_list
