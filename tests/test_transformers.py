from ferdelance_shared.transformers.scaling import FederatedMinMaxScaler
from ferdelance_shared.artifacts import QueryTransformer

from sklearn.preprocessing import MinMaxScaler

import pandas as pd
import os

PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, 'california.csv')


class TestTransformers:

    def test_build_query_transformer(self):
        fmms = FederatedMinMaxScaler('Latitude', 'Latitude2', (0.5, 2.0))
        qt: QueryTransformer = fmms.build()

        print(qt.dict())

        assert qt.name == FederatedMinMaxScaler.__name__
        assert qt.features_in == fmms.features_in
        assert qt.features_out == fmms.features_out
        assert len(qt.parameters) == 1
        assert 'feature_range' in qt.parameters
        assert qt.parameters['feature_range'] == (0.5, 2.0)

    def test_scaling_one_feature(self):
        df_a = pd.read_csv(PATH_CALIFORNIA)
        df_b = df_a.copy()

        mms = MinMaxScaler()
        mms.fit(df_a[['Latitude']])
        df_a[['Latitude_scaled']] = mms.transform(df_a[['Latitude']])

        fmms = FederatedMinMaxScaler('Latitude', 'Latitude_scaled')
        fmms.fit(df_b)
        df_b = fmms.transform(df_b)

        assert df_a['Latitude_scaled'].sum() == df_b['Latitude_scaled'].sum()
        assert df_a['Latitude_scaled'].mean() == df_b['Latitude_scaled'].mean()

    def test_scaling_multiple_features(self):
        df_a = pd.read_csv(PATH_CALIFORNIA)
        df_b = df_a.copy()

        mms = MinMaxScaler()
        mms.fit(df_a[['Latitude', 'Longitude']])
        df_a[['Latitude_scaled', 'Longitude_scaled']] = mms.transform(df_a[['Latitude', 'Longitude']])

        fmms = FederatedMinMaxScaler(['Latitude', 'Longitude'], ['Latitude_scaled', 'Longitude_scaled'])
        fmms.fit(df_b)
        df_b = fmms.transform(df_b)

        assert df_a['Latitude_scaled'].sum() == df_b['Latitude_scaled'].sum()
        assert df_a['Latitude_scaled'].mean() == df_b['Latitude_scaled'].mean()
        assert df_a['Longitude_scaled'].sum() == df_b['Longitude_scaled'].sum()
        assert df_a['Longitude_scaled'].mean() == df_b['Longitude_scaled'].mean()
