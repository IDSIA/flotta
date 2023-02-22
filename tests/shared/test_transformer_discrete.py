from ferdelance.schemas.queries import QueryTransformer, QueryFeature
from ferdelance.schemas.transformers import (
    FederatedKBinsDiscretizer,
    FederatedBinarizer,
    FederatedLabelBinarizer,
    FederatedOneHotEncoder,
)

import pandas as pd
import os

PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, "california.csv")


def test_kbin_build():
    f = QueryFeature(name="Housing")
    fkbd = FederatedKBinsDiscretizer(f, "HouseAgeBinary", n_bins=7, strategy="quantile", random_state=42)
    qt: QueryTransformer = fkbd.build()

    assert len(qt.parameters) == 3
    assert "n_bins" in qt.parameters
    assert "strategy" in qt.parameters
    assert "random_state" in qt.parameters
    assert qt.parameters["n_bins"] == 7
    assert qt.parameters["strategy"] == "quantile"
    assert qt.parameters["random_state"] == 42


def test_bin_build():
    f = QueryFeature(name="HouseAge")
    fb = FederatedBinarizer(f, "HouseAgeBinary", threshold=0.3)
    qt: QueryTransformer = fb.build()

    assert len(qt.parameters) == 1
    assert "threshold" in qt.parameters
    assert qt.parameters["threshold"] == 0.3


def test_lbin_build():
    f = QueryFeature(name="HouseAge")
    fb = FederatedLabelBinarizer(f, "HouseAgeBinary", neg_label=7, pos_label=9)
    qt: QueryTransformer = fb.build()

    assert len(qt.parameters) == 2
    assert "neg_label" in qt.parameters
    assert "pos_label" in qt.parameters
    assert qt.parameters["neg_label"] == 7
    assert qt.parameters["pos_label"] == 9


def test_ohe_build():
    f = QueryFeature(name="HouseAge")
    fmms = FederatedOneHotEncoder(
        f,
        "HouseAgeBinary",
        categories=[1, 2, 3],
        drop="first",
        handle_unknown="ignore",
        min_frequency=1,
        max_categories=3,
    )
    qt: QueryTransformer = fmms.build()

    assert len(qt.parameters) == 5
    assert "categories" in qt.parameters
    assert "drop" in qt.parameters
    assert "handle_unknown" in qt.parameters
    assert "min_frequency" in qt.parameters
    assert "max_categories" in qt.parameters

    assert qt.parameters["categories"] == [1, 2, 3]
    assert qt.parameters["drop"] == "first"
    assert qt.parameters["handle_unknown"] == "ignore"
    assert qt.parameters["min_frequency"] == 1
    assert qt.parameters["max_categories"] == 3


def test_kbin_one_feature():
    df = pd.read_csv(PATH_CALIFORNIA)

    f = QueryFeature(name="HouseAge")
    fkbd = FederatedKBinsDiscretizer(f, "HouseAgeBin", 10, random_state=42)

    df = fkbd.transform(df)

    assert df.shape[1] == 9

    x = df[["HouseAgeBin"]].groupby("HouseAgeBin").size()

    assert x.shape[0] == 10


def test_bin_one_feature():
    df = pd.read_csv(PATH_CALIFORNIA)

    f = QueryFeature(name="AveRooms")
    fb = FederatedBinarizer(f, "MoreThanThree", 3.0)

    df = fb.transform(df)

    assert df.shape[1] == 9
    assert df[["MoreThanThree"]].sum()[0] == 20185.0


def test_lbin_one_feature():
    df = pd.read_csv(PATH_CALIFORNIA)

    f1 = QueryFeature(name="HouseAge")
    f2 = QueryFeature(name="HouseAgeBin")
    fb = FederatedBinarizer(f1, f2, 30.0)
    df = fb.transform(df)

    flb = FederatedLabelBinarizer(f2, "HouseAgeLabel", -1, 1)
    df = flb.transform(df)

    # TODO: what if we binarize more columns or more values?

    assert df.shape[1] == 10
    assert df[["HouseAgeLabel"]].sum()[0] == -1650
