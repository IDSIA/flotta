from ferdelance.core.environment import Environment
from ferdelance.core.queries import QueryFeature
from ferdelance.core.transformers import (
    FederatedKBinsDiscretizer,
    FederatedBinarizer,
    FederatedLabelBinarizer,
)

from . import run

from pathlib import Path

import pandas as pd
import os

PATH_DIR = Path(os.path.abspath(os.path.dirname(__file__)))
PATH_CALIFORNIA = PATH_DIR / "california.csv"


def test_kbin_one_feature():
    df = pd.read_csv(PATH_CALIFORNIA)

    f_in = QueryFeature("HouseAge")
    f_out = QueryFeature("HouseAgeBin")
    fkbd = FederatedKBinsDiscretizer(features_in=[f_in], features_out=[f_out], n_bins=10, random_state=42)

    print(fkbd.json())

    df = run(df, fkbd)

    assert df is not None
    assert df.shape[1] == 9

    x = df[["HouseAgeBin"]].groupby("HouseAgeBin").size()

    assert x.shape[0] == 10


def test_bin_one_feature():
    df = pd.read_csv(PATH_CALIFORNIA)

    f_in = QueryFeature("AveRooms")
    f_out = QueryFeature("MoreThanThree")
    fb = FederatedBinarizer(features_in=[f_in], features_out=[f_out], threshold=3.0)

    df = run(df, fb)

    assert df is not None
    assert df.shape[1] == 9
    assert df[["MoreThanThree"]].sum()[0] == 20185.0


def test_lbin_one_feature():
    df = pd.read_csv(PATH_CALIFORNIA)

    f1 = QueryFeature("HouseAge")
    f2 = QueryFeature("HouseAgeBin")
    f3 = QueryFeature("HouseAgeLabel")

    fb = FederatedBinarizer(features_in=[f1], features_out=[f2], threshold=30.0)

    env = Environment("", "", "", Path("."))
    env.X_tr = df

    env, _ = fb.transform(env)

    assert env.X_tr is not None

    flb = FederatedLabelBinarizer(features_in=[f2], features_out=[f3], neg_label=-1, pos_label=1)

    env, _ = flb.transform(env)

    assert env.X_tr is not None
    assert env.Y_tr is not None

    # TODO: what if we binarize more columns or more values?

    assert df.shape[1] == 8
    assert f3.name not in env.X_tr.columns
    assert f3.name in env.Y_tr.columns
    assert env.Y_tr[f3.name].sum() == -1650
