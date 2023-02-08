from ferdelance.schemas.datasources import AggregatedDataSource, Feature, DataSource
from ferdelance.schemas.artifacts.dtypes import DataType


def test_aggregation():

    ds_name = "ds1"
    ds_id = "ds1"

    ds1 = DataSource(
        client_id="client1",
        datasource_id=ds_id,
        datasource_hash=ds_name,
        name=ds_name,
        n_records=10,
        n_features=2,
        features=[
            Feature(name="feature1", dtype=DataType.NUMERIC.name, v_mean=0.5),
            Feature(name="feature2", dtype=DataType.NUMERIC.name, v_mean=0.6),
        ],
    )

    ds_name = "ds2"
    ds_id = "ds2"

    ds2 = DataSource(
        client_id="client1",
        datasource_id=ds_id,
        datasource_hash=ds_name,
        name=ds_name,
        n_records=15,
        n_features=2,
        features=[
            Feature(name="feature1", dtype=DataType.NUMERIC.name, v_mean=0.7),
            Feature(name="feature3", dtype=DataType.NUMERIC.name, v_mean=0.4),
        ],
    )

    ds_name = "ds3"
    ds_id = "ds3"

    ds3 = DataSource(
        client_id="client2",
        datasource_id=ds_id,
        datasource_hash=ds_name,
        name=ds_name,
        n_records=15,
        n_features=2,
        features=[
            Feature(name="feature1", dtype=DataType.NUMERIC.name, v_mean=0.5),
            Feature(name="feature2", dtype=DataType.NUMERIC.name, v_mean=0.7),
            Feature(name="feature4", dtype=DataType.NUMERIC.name, v_mean=1.0),
        ],
    )

    ads = AggregatedDataSource.aggregate([ds1, ds2, ds3])

    assert ads.features_by_name["feature1"].n_datasources == 3
    assert ads.features_by_name["feature2"].n_datasources == 2
    assert ads.features_by_name["feature3"].n_datasources == 1
    assert ads.features_by_name["feature4"].n_datasources == 1

    assert ads.n_clients == 2
    assert ads.n_datasources == 3
    assert ads.n_features == 4
    assert ads.n_records == 40
