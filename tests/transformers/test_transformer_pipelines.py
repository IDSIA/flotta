from ferdelance.core.environment import Environment
from ferdelance.core.queries import QueryFeature
from ferdelance.core.transformers import (
    FederatedDrop,
    FederatedRename,
    FederatedPipeline,
    FederatedKBinsDiscretizer,
    FederatedBinarizer,
    FederatedLabelBinarizer,
    FederatedOneHotEncoder,
)

from pathlib import Path

import pandas as pd
import os

PATH_DIR = Path(os.path.abspath(os.path.dirname(__file__)))
PATH_CALIFORNIA = PATH_DIR / "california.csv"


def test_pipeline():
    latitude = QueryFeature("Latitude")
    longitude = QueryFeature("Longitude")

    med_inc = QueryFeature("MedInc")
    med_inc_th = QueryFeature("MedIncThresholded")

    label = QueryFeature("Label")
    med_inc_label = QueryFeature("MedIncLabel")
    house_age = QueryFeature("HouseAge")
    ave_rooms = QueryFeature("AveRooms")
    ave_beds = QueryFeature("AveBedrms")
    pop = QueryFeature("Population")
    ave_occ = QueryFeature("AveOccup")
    ave_occ_bins = QueryFeature("AveOccupBins")

    age_bins = QueryFeature("HouseAgeBins")
    room_bins = QueryFeature("AveRoomsBins")
    beds_bins = QueryFeature("AveBedrmsBins")
    pop_bins = QueryFeature("PopulationBins")
    occu_bins = QueryFeature("AveOccupBins")

    one_hots = [
        QueryFeature("OC1"),
        QueryFeature("OC2"),
        QueryFeature("OC3"),
    ]

    pipe = FederatedPipeline(
        stages=[
            # remove unused features
            FederatedDrop(features_in=[latitude, longitude]),
            # prepare label feature
            FederatedPipeline(
                stages=[
                    FederatedBinarizer(features_in=[med_inc], features_out=[med_inc_th]),
                    FederatedLabelBinarizer(features_in=[med_inc_th], features_out=[label], pos_label=1, neg_label=-1),
                    FederatedDrop(features_in=[med_inc, med_inc_th]),
                    FederatedRename(features_in=[label], features_out=[med_inc_label]),
                ]
            ),
            # prepare data features
            FederatedPipeline(
                stages=[
                    FederatedKBinsDiscretizer(
                        features_in=[house_age],
                        features_out=[age_bins],
                        n_bins=4,
                        strategy="uniform",
                        random_state=42,
                    ),
                    FederatedKBinsDiscretizer(
                        features_in=[ave_rooms],
                        features_out=[room_bins],
                        n_bins=10,
                        strategy="quantile",
                        random_state=42,
                    ),
                    FederatedKBinsDiscretizer(
                        features_in=[ave_beds],
                        features_out=[beds_bins],
                        n_bins=10,
                        strategy="quantile",
                        random_state=42,
                    ),
                    FederatedKBinsDiscretizer(
                        features_in=[pop],
                        features_out=[pop_bins],
                        n_bins=4,
                        strategy="kmeans",
                        random_state=42,
                    ),
                    FederatedKBinsDiscretizer(
                        features_in=[ave_occ],
                        features_out=[occu_bins],
                        n_bins=3,
                        strategy="uniform",
                        random_state=42,
                    ),
                    FederatedOneHotEncoder(features_in=[ave_occ_bins], features_out=one_hots),
                    FederatedDrop(features_in=[house_age, ave_rooms, ave_beds, pop, ave_occ, ave_occ_bins]),
                ]
            ),
        ]
    )

    env = Environment("", "")
    env.X_tr = pd.read_csv(PATH_CALIFORNIA)

    env, _ = pipe.transform(env)

    assert env.X_tr is not None
    assert env.Y_tr is not None

    assert env.X_tr.shape == (20640, 7)
    assert len(env.X_tr.columns) == 7
    for c in [
        "HouseAgeBins",
        "AveRoomsBins",
        "AveBedrmsBins",
        "PopulationBins",
        "AveOccupBins_0",
        "AveOccupBins_1",
        "AveOccupBins_2",
    ]:
        assert c in env.X_tr.columns

    x_mean = env.X_tr.mean(axis=0)
    y_mean = env.Y_tr.mean(axis=0)

    assert y_mean.MedIncLabel == -0.1631782945736434

    assert x_mean.HouseAgeBins == 1.6400193798449612
    assert x_mean.AveRoomsBins == 4.500048449612403
    assert x_mean.AveBedrmsBins == 4.500290697674418
    assert x_mean.PopulationBins == 0.30925387596899223

    assert x_mean.AveOccupBins_0 == 0.9998546511627907
    assert x_mean.AveOccupBins_1 == 9.689922480620155e-05
    assert x_mean.AveOccupBins_2 == 4.8449612403100775e-05

    assert x_mean.AveOccupBins_0 + x_mean.AveOccupBins_1 + x_mean.AveOccupBins_2 == 1.0
