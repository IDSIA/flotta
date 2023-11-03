from ferdelance.core.queries import QueryFeature
from ferdelance.core.transformers import (
    FederatedMinMaxScaler,
    FederatedStandardScaler,
)

from . import run

from sklearn.preprocessing import MinMaxScaler, StandardScaler

import pandas as pd
import os


PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, "california.csv")


def test_mms_scaling_one_feature():
    df_a = pd.read_csv(PATH_CALIFORNIA)
    df_b = df_a.copy()

    mms = MinMaxScaler()
    mms.fit(df_a[["Latitude"]])
    df_a[["Latitude_scaled"]] = mms.transform(df_a[["Latitude"]])

    f_in = QueryFeature("Latitude", "float")
    f_out = QueryFeature("Latitude_scaled", "float")

    fmms = FederatedMinMaxScaler(features_in=[f_in], features_out=[f_out])

    df_b = run(df_b, fmms)

    assert df_b is not None
    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()


def test_ssc_scaling_one_feature():
    df_a = pd.read_csv(PATH_CALIFORNIA)
    df_b = df_a.copy()

    ssc = StandardScaler()
    ssc.fit(df_a[["Latitude"]])
    df_a[["Latitude_scaled"]] = ssc.transform(df_a[["Latitude"]])

    f_in = QueryFeature("Latitude", "float")
    f_out = QueryFeature("Latitude_scaled", "float")

    fssc = FederatedStandardScaler(features_in=[f_in], features_out=[f_out])

    df_b = run(df_b, fssc)

    assert df_b is not None
    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()


def test_mms_scaling_multiple_features():
    df_a = pd.read_csv(PATH_CALIFORNIA)
    df_b = df_a.copy()

    mms = MinMaxScaler()
    mms.fit(df_a[["Latitude", "Longitude"]])
    df_a[["Latitude_scaled", "Longitude_scaled"]] = mms.transform(df_a[["Latitude", "Longitude"]])

    f1_in = QueryFeature("Latitude", "float")
    f2_in = QueryFeature("Longitude", "float")
    f1_out = QueryFeature("Latitude_scaled", "float")
    f2_out = QueryFeature("Longitude_scaled", "float")

    fmms = FederatedMinMaxScaler(features_in=[f1_in, f2_in], features_out=[f1_out, f2_out])

    df_b = run(df_b, fmms)

    assert df_b is not None
    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()
    assert df_a["Longitude_scaled"].sum() == df_b["Longitude_scaled"].sum()
    assert df_a["Longitude_scaled"].mean() == df_b["Longitude_scaled"].mean()


def test_sc_scaling_multiple_features():
    df_a = pd.read_csv(PATH_CALIFORNIA)
    df_b = df_a.copy()

    ssc = StandardScaler()
    ssc.fit(df_a[["Latitude", "Longitude"]])
    df_a[["Latitude_scaled", "Longitude_scaled"]] = ssc.transform(df_a[["Latitude", "Longitude"]])

    f1_in = QueryFeature("Latitude", "float")
    f2_in = QueryFeature("Longitude", "float")
    f1_out = QueryFeature("Latitude_scaled", "float")
    f2_out = QueryFeature("Longitude_scaled", "float")

    fssc = FederatedStandardScaler(features_in=[f1_in, f2_in], features_out=[f1_out, f2_out])

    df_b = run(df_b, fssc)

    assert df_b is not None
    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()
    assert df_a["Longitude_scaled"].sum() == df_b["Longitude_scaled"].sum()
    assert df_a["Longitude_scaled"].mean() == df_b["Longitude_scaled"].mean()


"""
TODO
def test_mms_save_and_reload():
    # TODO

    df = pd.read_csv(PATH_CALIFORNIA)
    df_a = df.copy()
    df_b = df.copy()

    f_in = QueryFeature("Latitude", "float")
    f_out = QueryFeature("Latitude_scaled", "float")

    fmms = FederatedMinMaxScaler(features_in=[f_in], features_out=[f_out])

    TF_PATH = os.path.join(".", "mms.transformer")

    save(fmms, TF_PATH)

    loaded: Transformer = run(TF_PATH)

    assert isinstance(loaded, FederatedMinMaxScaler)
    assert fmms.features_in == loaded.features_in
    assert fmms.features_out == loaded.features_out

    df_a = fmms.transform(df_a)[0]
    df_b = loaded.transform(df_b)[0]

    assert df_a is not None
    assert df_b is not None
    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()

    os.remove(TF_PATH)


def test_ssc_save_and_reload():
    # TODO

    df = pd.read_csv(PATH_CALIFORNIA)
    df_a = df.copy()
    df_b = df.copy()

    f_in = QueryFeature("Latitude", "float")
    f_out = QueryFeature("Latitude_scaled", "float")

    fssc = FederatedStandardScaler(features_in=[f_in], features_out=[f_out])

    TF_PATH = os.path.join(".", "mms.transformer")

    save(fssc, TF_PATH)

    loaded: Transformer = run(TF_PATH)

    assert isinstance(loaded, FederatedStandardScaler)
    assert fssc.features_in == loaded.features_in
    assert fssc.features_out == loaded.features_out

    df_a = fssc.transform(df_a)[0]
    df_b = loaded.transform(df_b)[0]

    assert df_a is not None
    assert df_b is not None
    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()

    os.remove(TF_PATH)
"""
