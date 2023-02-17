from ferdelance.schemas.queries import QueryTransformer, QueryFeature
from ferdelance.schemas.transformers import (
    apply_transformer,
    FederatedPipeline,
    FederatedKBinsDiscretizer,
    FederatedDrop,
    FederatedBinarizer,
    FederatedLabelBinarizer,
    FederatedRename,
)

import pandas as pd
import os

PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, "california.csv")


def test_apply_transformer():
    f = QueryFeature(name="HouseAge", dtype="float")
    tr = FederatedKBinsDiscretizer(f, "HouseAgeBin", 10, random_state=42)
    qt: QueryTransformer = tr.build()

    df = pd.read_csv(PATH_CALIFORNIA)

    df = apply_transformer(qt, df)

    x = df[["HouseAgeBin"]].groupby("HouseAgeBin").size()

    assert x.shape[0] == 10
    assert df["HouseAgeBin"].mean() == 4.877858527131783


def test_apply_pipeline():

    med_inc = QueryFeature(name="MedInc", dtype="float")
    med_inc_th = QueryFeature(name="MedIncThresholded", dtype="float")

    pipe = FederatedPipeline(
        [
            # remove unused features
            FederatedDrop(["Latitude", "Longitude"]),
            # prepare label feature
            FederatedPipeline(
                [
                    FederatedBinarizer(med_inc, med_inc_th),
                    FederatedLabelBinarizer(med_inc_th, "Label", pos_label=1, neg_label=-1),
                    FederatedDrop([med_inc, med_inc_th]),
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
