from typing import Any

from ferdelance.logging import get_logger
from ferdelance.schemas.plans.local.core import LocalPlan, GenericModel, Metrics

from sklearn.model_selection import StratifiedKFold, KFold

import pandas as pd


LOGGER = get_logger(__name__)


class LocalCrossValidation(LocalPlan):
    """Execution plan that duplicates the input data and apply two different plans to them."""

    def __init__(
        self,
        label: str,
        local_plan: LocalPlan,
        folds: int = 10,
        stratified: bool = True,
        shuffle: bool = True,
        source: str = "test",
        random_seed: Any = None,
    ) -> None:
        super().__init__(LocalCrossValidation.__name__, label, random_seed, local_plan)

        self.folds: int = folds
        self.stratified: bool = stratified
        self.shuffle: bool = shuffle
        self.source: str = source

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "folds": self.folds,
            "stratified": self.stratified,
            "shuffle": self.shuffle,
            "source": self.source,
        }

    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> list[Metrics]:
        self.validate_input(df)

        if self.local_plan is None:
            raise ValueError("No local_plan assigned")

        if self.stratified:
            kf = StratifiedKFold(self.folds, shuffle=True, random_state=self.random_seed)
        else:
            kf = KFold(self.folds, shuffle=self.shuffle, random_state=self.random_seed)

        x = df
        y = df[self.label]

        metrics_list: list[Metrics] = list()

        fold = 0
        for tr, ts in kf.split(x, y):
            x_tr = x.iloc[tr].copy()
            x_ts = x.iloc[ts].copy()

            # TODO: copy initial model

            self.local_plan.run(
                x_tr,
                local_model,
                working_folder,
                artifact_id,
            )

            x_ts = x_ts.drop(self.label, axis=1).values
            y_ts = x_ts[self.label]

            metrics = local_model.eval(x_ts, y_ts)
            metrics.source = f"{self.source}_{fold}"
            metrics.artifact_id = artifact_id

            metrics_list.append(metrics)

            fold += 1

        return metrics_list
