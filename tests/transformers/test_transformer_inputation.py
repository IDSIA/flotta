from ferdelance.core.queries import QueryFeature
from ferdelance.core.transformers import FederatedSimpleImputer

from . import run

from sklearn.impute import SimpleImputer

import numpy as np
import pandas as pd
import os

PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, "california.csv")


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

    f_in = QueryFeature("HouseAge")
    f_out = QueryFeature("HouseAgeImputed")

    fsi = FederatedSimpleImputer(features_in=[f_in], features_out=[f_out])

    df_b = run(df_b, fsi)

    assert df_b is not None

    assert df_a["HouseAgeImputed"].mean() == df_b["HouseAgeImputed"].mean()
    assert df_a["HouseAgeImputed"].sum() == df_b["HouseAgeImputed"].sum()
