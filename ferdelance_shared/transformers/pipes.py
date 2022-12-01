from typing import Any

from .core import Transformer

import pandas as pd


class FederatedPipeline(Transformer):

    def __init__(self, stages: list[Transformer]) -> None:
        super().__init__(FederatedPipeline.__name__)

        self.stages: list[Transformer] = stages

    def dict(self) -> dict[str, Any]:
        return super().dict() | {
            'stages': [
                stage.dict() for stage in self.stages
            ]
        }

    def load(self, path: str) -> None:
        with open(path, 'rb') as f:
            data = self._load(f)
            self.stages = data['stages']  # TODO: this is wrong and each stage should be able to 'recreate' themselves

    def fit(self, df: pd.DataFrame) -> None:
        for stage in self.stages:
            stage.fit(df)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for stage in self.stages:
            df = stage.transform(df)
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
