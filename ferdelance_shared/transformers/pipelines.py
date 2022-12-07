from typing import Any

from .core import Transformer

import pandas as pd


class FederatedPipeline(Transformer):
    """A pipeline that can be used to group sequence of Transformers. 
    The stages of the pipeline will be applied in sequence to the input data.

    A pipeline can also be nested inside another pipeline.
    """

    def __init__(self, stages: list[Transformer]) -> None:
        """
        :param stages:
            List of all the stages that will be used by this pipeline.
        """
        super().__init__(FederatedPipeline.__name__)

        self.stages: list[Transformer] = stages

    def params(self) -> dict[str, Any]:
        return super().params() | {
            'stages_params': [s.params() for s in self.stages]
        }

    def dict(self) -> dict[str, Any]:
        return super().dict() | {
            'stages': [
                stage.dict() for stage in self.stages
            ]
        }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for stage in self.stages:
            df = stage.transform(df)
        return df

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()
