from ferdelance.schemas.queries import Query, QueryFilter, Operations, QueryFeature
from ferdelance.schemas.datasources import Feature, DataSource
from ferdelance.schemas.queries.operations import Operations
from ferdelance.schemas.transformers import FederatedBinarizer

DS1_NAME, DS1_ID = "data_source_1", "ds1"
DS2_NAME, DS2_ID = "data_source_2", "ds2"


def feature1() -> Feature:
    return Feature(
        name="feature1",
        dtype="float",
        v_mean=None,
        v_std=None,
        v_min=None,
        v_p25=None,
        v_p50=None,
        v_p75=None,
        v_max=None,
        v_miss=None,
    )


def feature2() -> Feature:
    return Feature(
        name="feature2",
        dtype="int",
        v_mean=None,
        v_std=None,
        v_min=None,
        v_p25=None,
        v_p50=None,
        v_p75=None,
        v_max=None,
        v_miss=None,
    )


def feature3() -> Feature:
    return Feature(
        name="feature3",
        dtype="int",
        v_mean=None,
        v_std=None,
        v_min=None,
        v_p25=None,
        v_p50=None,
        v_p75=None,
        v_max=None,
        v_miss=None,
    )


def datasource1() -> DataSource:
    features = [
        feature1(),
        feature2(),
    ]
    return DataSource(
        datasource_id=DS1_ID,
        datasource_hash="1",
        name=DS1_NAME,
        n_records=1000,
        n_features=len(features),
        client_id="client1",
        features=features,
        tokens=[""],
    )


def datasource2() -> DataSource:
    features = [
        feature3(),
    ]
    return DataSource(
        datasource_id=DS2_ID,
        datasource_hash="2",
        name=DS2_NAME,
        n_records=1000,
        n_features=len(features),
        client_id="client1",
        features=features,
        tokens=[""],
    )


def test_features():

    f1 = feature1()

    fq1 = f1.qf()

    assert fq1 == f1.qf()
    assert fq1 is not f1
    assert fq1 is not f1.qf()
    assert hash(fq1) == hash(f1.qf())

    features = [fq1]

    assert fq1 in features

    features.remove(fq1)

    assert not features

    features = [fq1]
    features.remove(f1.qf())

    assert not features


def test_query_composition():

    ds: DataSource = datasource1()

    f1: QueryFeature = ds.features[0].qf()
    f2: QueryFeature = ds.features[1].qf()

    # initial query
    q: Query = ds.extract()

    assert len(q.stages) == 1

    s = q.current()

    assert s.transformer is None
    assert len(s.features) == 2
    assert f1 in s.features
    assert f2 in s.features

    # adding a filter

    f: QueryFilter = q[f1] > 3

    assert isinstance(f, QueryFilter)

    q.add(f)

    assert len(q.stages) == 2

    s = q.current()

    assert f1 in s.features
    assert f2 in s.features

    assert s.transformer is not None

    params = s.transformer.params()

    assert "feature" in params
    assert params["feature"] == f1.name
    assert "operation" in params
    assert params["operation"] == Operations.NUM_GREATER_THAN.name
    assert "value" in params
    assert params["value"] == "3"

    # adding a transformer

    b = FederatedBinarizer(f1, "binary", 0.5)

    q.add(b)

    assert len(q.stages) == 3

    s = q.current()

    assert f1 not in s.features
    assert f2 in s.features
    assert "binary" in s.features

    assert s.transformer is not None

    params = s.transformer.params()

    assert "features_in" in params
    assert len(params["features_in"]) == 1
    assert params["features_in"][0] == f1.name

    assert "features_out" in params
    assert len(params["features_out"]) == 1
    assert params["features_out"][0] == "binary"

    assert params["threshold"] == 0.5
