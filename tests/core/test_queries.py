from flotta.core.queries import Query, QueryFilter, QueryFeature
from flotta.core.transformers import FederatedBinarizer
from flotta.schemas.datasources import Feature, DataSource

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
        id=DS1_ID,
        hash="1",
        name=DS1_NAME,
        n_records=1000,
        n_features=len(features),
        component_id="client1",
        features=features,
        tokens=[""],
    )


def datasource2() -> DataSource:
    features = [
        feature3(),
    ]
    return DataSource(
        id=DS2_ID,
        hash="2",
        name=DS2_NAME,
        n_records=1000,
        n_features=len(features),
        component_id="client1",
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

    q = q.add(f)

    assert len(q.stages) == 2

    s = q.current()

    assert f1 in s.features
    assert f2 in s.features

    assert s.transformer is not None

    # adding a transformer
    f_out = QueryFeature("binary")

    b = FederatedBinarizer(features_in=[f1], features_out=[f_out], threshold=0.5)

    q = q.add(b)

    assert len(q.stages) == 3

    s = q.current()

    assert f1 not in s.features
    assert f2 in s.features
    assert "binary" in s.features

    assert s.transformer is not None
