from .core import (
    GenericTransformer,
)
from ..artifacts import (
    QueryFeature,
    QueryTransformer,
)

from sklearn.preprocessing import MinMaxScaler
from pydantic import BaseModel

import pandas as pd


class FederatedMinMaxScalerParameters(BaseModel):
    feature_range: tuple = (0, 1)


class FederatedMinMaxScaler(GenericTransformer):

    def __init__(self, feature: QueryFeature, params: FederatedMinMaxScalerParameters) -> None:
        super().__init__()

        self.params = params
        self.scaler = MinMaxScaler(feature_range=params.feature_range)
        self.feature = feature

    def fit(self, df: pd.DataFrame) -> None:
        self.scaler.fit(df)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.scaler.transform(df[[self.feature.feature_name]])

    def build(self) -> QueryTransformer:
        return QueryTransformer(
            feature=self.feature,
            name=FederatedMinMaxScaler.__name__,
            parameters=self.params.dict(),
        )
