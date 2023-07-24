from ferdelance.schemas.queries import QueryFeature, QueryTransformer
from ferdelance.schemas.transformers import (
    Transformer,
    FederatedMinMaxScaler,
    FederatedStandardScaler,
    save,
    run,
)
from sklearn.preprocessing import MinMaxScaler, StandardScaler

import pandas as pd
import os

PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, "california.csv")


def test_mms_build():
    f = QueryFeature(name="Latitude", dtype="float")

    fmms = FederatedMinMaxScaler(f, "Latitude2", (0.5, 2.0))
    qt: QueryTransformer = fmms.build()

    assert len(qt.parameters) == 1
    assert "feature_range" in qt.parameters
    assert qt.parameters["feature_range"] == (0.5, 2.0)


def test_ssc_build():
    f = QueryFeature(name="Latitude", dtype="float")

    fssc = FederatedStandardScaler(f, "Latitude2", with_mean=False, with_std=False)
    qt = fssc.build()

    assert qt.name == FederatedStandardScaler.__name__
    assert len(qt.parameters) == 2
    assert "with_mean" in qt.parameters
    assert "with_std" in qt.parameters
    assert qt.parameters["with_mean"] is False
    assert qt.parameters["with_std"] is False


def test_mms_scaling_one_feature():
    df_a = pd.read_csv(PATH_CALIFORNIA)
    df_b = df_a.copy()

    mms = MinMaxScaler()
    mms.fit(df_a[["Latitude"]])
    df_a[["Latitude_scaled"]] = mms.transform(df_a[["Latitude"]])

    f = QueryFeature(name="Latitude", dtype="float")

    fmms = FederatedMinMaxScaler(f, "Latitude_scaled")
    df_b = fmms.transform(df_b)

    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()


def test_ssc_scaling_one_feature():
    df_a = pd.read_csv(PATH_CALIFORNIA)
    df_b = df_a.copy()

    ssc = StandardScaler()
    ssc.fit(df_a[["Latitude"]])
    df_a[["Latitude_scaled"]] = ssc.transform(df_a[["Latitude"]])

    f = QueryFeature(name="Latitude", dtype="float")

    fssc = FederatedStandardScaler(f, "Latitude_scaled")
    df_b = fssc.transform(df_b)

    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()


def test_mms_scaling_multiple_features():
    df_a = pd.read_csv(PATH_CALIFORNIA)
    df_b = df_a.copy()

    mms = MinMaxScaler()
    mms.fit(df_a[["Latitude", "Longitude"]])
    df_a[["Latitude_scaled", "Longitude_scaled"]] = mms.transform(df_a[["Latitude", "Longitude"]])

    f1 = QueryFeature(name="Latitude", dtype="float")
    f2 = QueryFeature(name="Longitude", dtype="float")

    fmms = FederatedMinMaxScaler([f1, f2], ["Latitude_scaled", "Longitude_scaled"])
    df_b = fmms.transform(df_b)

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

    f1 = QueryFeature(name="Latitude", dtype="float")
    f2 = QueryFeature(name="Longitude", dtype="float")

    fssc = FederatedStandardScaler([f1, f2], ["Latitude_scaled", "Longitude_scaled"])
    df_b = fssc.transform(df_b)

    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()
    assert df_a["Longitude_scaled"].sum() == df_b["Longitude_scaled"].sum()
    assert df_a["Longitude_scaled"].mean() == df_b["Longitude_scaled"].mean()


def test_mms_save_and_reload():
    df = pd.read_csv(PATH_CALIFORNIA)
    df_a = df.copy()
    df_b = df.copy()

    f = QueryFeature(name="Latitude", dtype="float")

    fmms = FederatedMinMaxScaler(f, "Latitude_scaled")

    TF_PATH = os.path.join(".", "mms.transformer")

    save(fmms, TF_PATH)

    loaded: Transformer = run(TF_PATH)

    assert isinstance(loaded, FederatedMinMaxScaler)
    assert fmms.name == loaded.name
    assert fmms.features_in == loaded.features_in
    assert fmms.features_out == loaded.features_out
    assert fmms.params() == loaded.params()

    df_a = fmms.transform(df_a)
    df_b = loaded.transform(df_b)

    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()

    os.remove(TF_PATH)


def test_ssc_save_and_reload():
    df = pd.read_csv(PATH_CALIFORNIA)
    df_a = df.copy()
    df_b = df.copy()

    f = QueryFeature(name="Latitude", dtype="float")

    fssc = FederatedStandardScaler(f, "Latitude_scaled")

    TF_PATH = os.path.join(".", "mms.transformer")

    save(fssc, TF_PATH)

    loaded: Transformer = run(TF_PATH)

    assert isinstance(loaded, FederatedStandardScaler)
    assert fssc.name == loaded.name
    assert fssc.features_in == loaded.features_in
    assert fssc.features_out == loaded.features_out
    assert fssc.params() == loaded.params()

    df_a = fssc.transform(df_a)
    df_b = loaded.transform(df_b)

    assert df_a["Latitude_scaled"].sum() == df_b["Latitude_scaled"].sum()
    assert df_a["Latitude_scaled"].mean() == df_b["Latitude_scaled"].mean()

    os.remove(TF_PATH)
