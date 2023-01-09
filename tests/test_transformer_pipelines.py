from ferdelance.shared.transformers import (
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
PATH_CALIFORNIA = os.path.join(PATH_DIR, 'california.csv')


class TestTransformerPipelines:

    def test_pipeline(self):
        df = pd.read_csv(PATH_CALIFORNIA)

        pipe = FederatedPipeline([
            # remove unused features
            FederatedDrop(['Latitude', 'Longitude']),

            # prepare label feature
            FederatedPipeline([
                FederatedBinarizer('MedInc', 'MedIncThresholded'),
                FederatedLabelBinarizer('MedIncThresholded', 'Label', pos_label=1, neg_label=-1),
                FederatedDrop(['MedInc', 'MedIncThresholded']),
                FederatedRename('Label', 'MedIncLabel'),
            ]),
            # prepare data features
            FederatedPipeline([
                FederatedKBinsDiscretizer('HouseAge', 'HouseAgeBins', 4, 'uniform', random_state=42),
                FederatedKBinsDiscretizer('AveRooms', 'AveRoomsBins', 10, 'quantile', random_state=42),
                FederatedKBinsDiscretizer('AveBedrms', 'AveBedrmsBins', 10, 'quantile', random_state=42),
                FederatedKBinsDiscretizer('Population', 'PopulationBins', 4, 'kmeans', random_state=42),
                FederatedKBinsDiscretizer('AveOccup', 'AveOccupBins', 3, 'uniform', random_state=42),
                FederatedOneHotEncoder('AveOccupBins', ['OC1', 'OC2', 'OC3']),
                FederatedDrop(['HouseAge', 'AveRooms', 'AveBedrms', 'Population', 'AveOccup', 'AveOccupBins']),
            ]),
        ])

        df = pipe.transform(df)

        assert df.shape == (20640, 8)
        assert len(df.columns) == 8
        for c in ['MedIncLabel', 'HouseAgeBins', 'AveRoomsBins', 'AveBedrmsBins', 'PopulationBins', 'AveOccupBins_0', 'AveOccupBins_1', 'AveOccupBins_2']:
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
