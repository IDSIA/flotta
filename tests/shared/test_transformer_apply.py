from ferdelance.shared.transformers import (
    apply_transformer,
    FederatedPipeline,
    FederatedKBinsDiscretizer,
    FederatedDrop,
    FederatedBinarizer,
    FederatedLabelBinarizer,
    FederatedRename,
)
from ferdelance.schemas.artifacts import QueryTransformer

import pandas as pd
import os

PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, "california.csv")


def test_apply_transformer():
    tr = FederatedKBinsDiscretizer("HouseAge", "HouseAgeBin", 10, random_state=42)
    qt: QueryTransformer = tr.build()

    df = pd.read_csv(PATH_CALIFORNIA)

    df = apply_transformer(qt, df)

    x = df[["HouseAgeBin"]].groupby("HouseAgeBin").size()

    assert x.shape[0] == 10
    assert df["HouseAgeBin"].mean() == 4.877858527131783


def test_apply_pipeline():
    pipe = FederatedPipeline(
        [
            # remove unused features
            FederatedDrop(["Latitude", "Longitude"]),
            # prepare label feature
            FederatedPipeline(
                [
                    FederatedBinarizer("MedInc", "MedIncThresholded"),
                    FederatedLabelBinarizer("MedIncThresholded", "Label", pos_label=1, neg_label=-1),
                    FederatedDrop(["MedInc", "MedIncThresholded"]),
                    FederatedRename("Label", "MedIncLabel"),
                ]
            ),
        ]
    )

    qt: QueryTransformer = pipe.build()

    df = pd.read_csv(PATH_CALIFORNIA)

    df = apply_transformer(qt, df)

    assert "Latitude" not in df.columns
    assert "Longitude" not in df.columns
    assert "MedIncThresholded" not in df.columns
    assert "MedInc" not in df.columns
    assert "Label" not in df.columns

    assert "MedIncLabel" in df.columns

    labels = df.groupby("MedIncLabel").size()

    assert labels[-1] == 12004
    assert labels[1] == 8636
