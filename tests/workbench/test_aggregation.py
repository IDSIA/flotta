from ferdelance.schemas.datasources import AggregatedDataSource, Feature, DataSource


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
            Feature(
                name="feature1",
            ),
            Feature(
                name="feature2",
            ),
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
            Feature(
                name="feature1",
            ),
            Feature(
                name="feature2",
            ),
        ],
    )

    ads = AggregatedDataSource.aggregate([ds1, ds2])

    print(ads)
