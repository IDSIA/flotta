from ferdelance.schemas.queries import QueryFeature
from ferdelance.schemas.transformers import (
    FederatedDrop,
    FederatedRename,
    FederatedPipeline,
    FederatedKBinsDiscretizer,
    FederatedBinarizer,
    FederatedLabelBinarizer,
    FederatedOneHotEncoder,
)

import pandas as pd
import os

PATH_DIR = os.path.abspath(os.path.dirname(__file__))
PATH_CALIFORNIA = os.path.join(PATH_DIR, "california.csv")


def test_pipeline():
    df = pd.read_csv(PATH_CALIFORNIA)

    latitude = QueryFeature(name="Latitude")
    longitude = QueryFeature(name="Longitude")

    med_inc = QueryFeature(name="MedInc")
    med_inc_th = QueryFeature(name="MedIncThresholded")

    label = QueryFeature(name="Label")
    med_inc_label = QueryFeature(name="MedIncLabel")
    house_age = QueryFeature(name="HouseAge")
    ave_rooms = QueryFeature(name="AveRooms")
    ave_beds = QueryFeature(name="AveBedrms")
    pop = QueryFeature(name="Population")
    ave_occ = QueryFeature(name="AveOccup")
    ave_occ_bins = QueryFeature(name="AveOccupBins")

    pipe = FederatedPipeline(
        [
            # remove unused features
            FederatedDrop([latitude, longitude]),
            # prepare label feature
            FederatedPipeline(
                [
                    FederatedBinarizer(med_inc, med_inc_th),
                    FederatedLabelBinarizer(med_inc_th, label, pos_label=1, neg_label=-1),
                    FederatedDrop([med_inc, med_inc_th]),
                    FederatedRename(label, med_inc_label),
                ]
            ),
            # prepare data features
            FederatedPipeline(
                [
                    FederatedKBinsDiscretizer(house_age, "HouseAgeBins", 4, "uniform", random_state=42),
                    FederatedKBinsDiscretizer(ave_rooms, "AveRoomsBins", 10, "quantile", random_state=42),
                    FederatedKBinsDiscretizer(ave_beds, "AveBedrmsBins", 10, "quantile", random_state=42),
                    FederatedKBinsDiscretizer(pop, "PopulationBins", 4, "kmeans", random_state=42),
                    FederatedKBinsDiscretizer(ave_occ, "AveOccupBins", 3, "uniform", random_state=42),
                    FederatedOneHotEncoder(ave_occ_bins, ["OC1", "OC2", "OC3"]),
                    FederatedDrop([house_age, ave_rooms, ave_beds, pop, ave_occ, ave_occ_bins]),
                ]
            ),
        ]
    )

    df = pipe.transform(df)

    assert df.shape == (20640, 8)
    assert len(df.columns) == 8
    for c in [
        "MedIncLabel",
        "HouseAgeBins",
        "AveRoomsBins",
        "AveBedrmsBins",
        "PopulationBins",
        "AveOccupBins_0",
        "AveOccupBins_1",
        "AveOccupBins_2",
    ]:
        assert c in df.columns

    df_mean = df.mean(axis=0)

    assert df_mean.MedIncLabel == -0.1631782945736434
    assert df_mean.HouseAgeBins == 1.6400193798449612
    assert df_mean.AveRoomsBins == 4.500048449612403
    assert df_mean.AveBedrmsBins == 4.500290697674418
    assert df_mean.PopulationBins == 0.30925387596899223

    assert df_mean.AveOccupBins_0 == 0.9998546511627907
    assert df_mean.AveOccupBins_1 == 9.689922480620155e-05
    assert df_mean.AveOccupBins_2 == 4.8449612403100775e-05

    assert df_mean.AveOccupBins_0 + df_mean.AveOccupBins_1 + df_mean.AveOccupBins_2 == 1.0
