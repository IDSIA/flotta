from ferdelance.core.environment import Environment
from ferdelance.core.queries import QueryFeature
from ferdelance.core.transformers import (
    FederatedPipeline,
    FederatedKBinsDiscretizer,
    FederatedDrop,
    FederatedBinarizer,
    FederatedLabelBinarizer,
    FederatedRename,
)

from . import run

import pandas as pd
import os

PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, "california.csv")


def test_apply_transformer():
    f_in = QueryFeature("HouseAge", "float")
    f_out = QueryFeature("HouseAgeBin", "float")
    tr = FederatedKBinsDiscretizer(features_in=[f_in], features_out=[f_out], n_bins=10, random_state=42)

    df = pd.read_csv(PATH_CALIFORNIA)

    df = run(df, tr)

    assert df is not None

    x = df[["HouseAgeBin"]].groupby("HouseAgeBin").size()

    assert x.shape[0] == 10
    assert df["HouseAgeBin"].mean() == 4.877858527131783


def test_apply_pipeline():
    med_inc = QueryFeature("MedInc", "float")
    med_inc_th = QueryFeature("MedIncThresholded", "float")
    label_in = QueryFeature("Label")
    label = QueryFeature("MedIncLabel")

    lat = QueryFeature("Latitude")
    lon = QueryFeature("Longitude")

    pipe = FederatedPipeline(
        stages=[
            # remove unused features
            FederatedDrop([lat, lon]),
            # prepare label feature
            FederatedPipeline(
                stages=[
                    FederatedBinarizer(
                        features_in=[med_inc],
                        features_out=[med_inc_th],
                    ),
                    FederatedLabelBinarizer(
                        features_in=[med_inc_th],
                        features_out=[label_in],
                        pos_label=1,
                        neg_label=-1,
                    ),
                    FederatedDrop(
                        features_in=[med_inc, med_inc_th],
                    ),
                    FederatedRename(
                        features_in=[label_in],
                        features_out=[label],
                    ),
                ]
            ),
        ]
    )

    env = Environment("", "")
    env.X_tr = pd.read_csv(PATH_CALIFORNIA)

    env, _ = pipe.transform(env)

    assert env.X_tr is not None
    assert env.Y_tr is not None

    assert env.X_ts is None
    assert env.Y_ts is None

    assert "Latitude" not in env.X_tr.columns
    assert "Longitude" not in env.X_tr.columns
    assert "MedIncThresholded" not in env.X_tr.columns
    assert "MedInc" not in env.X_tr.columns

    assert "Label" not in env.X_tr.columns
    assert "Label" not in env.Y_tr.columns

    assert "MedIncLabel" not in env.X_tr.columns
    assert "MedIncLabel" in env.Y_tr.columns

    labels = env.Y_tr.groupby("MedIncLabel").size()

    assert labels[-1] == 12004
    assert labels[1] == 8636
