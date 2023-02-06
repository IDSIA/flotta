from ferdelance.schemas.transformers import (
    FederatedSimpleImputer,
)
from ferdelance.schemas.artifacts import QueryTransformer

from sklearn.impute import SimpleImputer

import numpy as np
import pandas as pd
import os

PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, "california.csv")


def test_imp_build():
    fsi = FederatedSimpleImputer("Latitude", "Latitude2", missing_values=7, fill_value=42)
    qt: QueryTransformer = fsi.build()

    assert len(qt.parameters) == 3
    assert "missing_values" in qt.parameters
    assert qt.parameters["missing_values"] == 7
    assert "strategy" in qt.parameters
    assert qt.parameters["strategy"] == "constant"
    assert "fill_value" in qt.parameters
    assert qt.parameters["fill_value"] == 42


def test_imp_one_feature():
    df = pd.read_csv(PATH_CALIFORNIA)

    mask_more_2 = df["HouseAge"] > 40
    mask_less_4 = df["HouseAge"] < 50

    df["HouseAge"][mask_more_2 & mask_less_4] = np.nan

    assert df["HouseAge"].mean() != 0

    df_a = df.copy()
    df_b = df.copy()

    si = SimpleImputer()
    df_a["HouseAgeImputed"] = si.fit_transform(df_a[["HouseAge"]])

    fsi = FederatedSimpleImputer("HouseAge", "HouseAgeImputed")
    df_b = fsi.transform(df_b)

    assert df_a["HouseAgeImputed"].mean() == df_b["HouseAgeImputed"].mean()
    assert df_a["HouseAgeImputed"].sum() == df_b["HouseAgeImputed"].sum()
