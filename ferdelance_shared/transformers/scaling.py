from .core import (
    GenericTransformer,
    convert_features_to_list
)
from ..artifacts import (
    QueryFeature,
    QueryTransformer,
)

from sklearn.preprocessing import MinMaxScaler

import pandas as pd
import pickle


class FederatedMinMaxScaler(GenericTransformer):

    def __init__(self,
                 features_in: QueryFeature | list[QueryFeature] | str | list[str],
                 features_out: QueryFeature | list[QueryFeature] | str | list[str],
                 feature_range: tuple = (0, 1)
                 ) -> None:
        super().__init__()

        self.scaler = MinMaxScaler(feature_range=feature_range)

        self.features_in: list[str] = convert_features_to_list(features_in)
        self.features_out: list[str] = convert_features_to_list(features_out)
        self.feature_range: tuple[float, float] = feature_range

        if len(self.features_in) != len(self.features_out):
            raise ValueError('Input and output features are not of the same length')

    def save(self, path: str) -> None:
        with open(path, 'wb') as f:
            pickle.dump({
                'features_in': self.features_in,
                'features_out': self.features_out,
                'feature_range': self.feature_range,
                'scaler': self.scaler,
            }, f)

    def load(self, path: str) -> None:
        with open(path, 'rb') as f:
            data = pickle.load(f)
            self.features_in = data['features_in']
            self.features_out = data['features_out']
            self.feature_range = data['feature_range']
            self.scaler = data['scaler']

    def aggregate(self) -> None:
        # TODO
        return super().aggregate()

    def fit(self, df: pd.DataFrame) -> None:
        self.scaler.fit(df)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.features_out] = self.scaler.transform(df[self.features_in])
        return df

    def build(self) -> QueryTransformer:
        return QueryTransformer(
            features_in=self.features_in,
            features_out=self.features_out,
            name=FederatedMinMaxScaler.__name__,
            parameters={
                'feature_range': self.feature_range
            },
        )
