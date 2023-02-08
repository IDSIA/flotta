from ferdelance.schemas.artifacts import Query, QueryFilter
from ferdelance.schemas.datasources import Feature, DataSource
from ferdelance.schemas.artifacts.operations import Operations

DS1_NAME, DS1_ID = "data_source_1", "ds1"
DS2_NAME, DS2_ID = "data_source_2", "ds2"


def feature1() -> Feature:
    return Feature(
        datasource_id=DS1_ID,
        datasource_name=DS1_NAME,
        feature_id="ds1-f1",
        name="feature1",
        dtype="str",
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
        datasource_id=DS1_ID,
        datasource_name=DS1_NAME,
        feature_id="ds1-f2",
        name="feature2",
        dtype="str",
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
        datasource_id=DS2_ID,
        datasource_name=DS2_NAME,
        feature_id="ds2-f3",
        name="feature3",
        dtype="str",
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

    assert fq1.datasource_id == f1.datasource_id
    assert fq1.feature_id == f1.feature_id
    assert fq1 == f1
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

    ds = datasource1()

    f1 = ds.features[0]
    f2 = ds.features[1]
    f3 = feature3()

    q1 = ds.all_features()

    assert len(q1.features) == 2
    assert f1 in q1.features
    assert f2 in q1.features
    assert q1.datasource_id == ds.datasource_id

    q2 = q1 - f1

    assert isinstance(q2, Query)
    assert q2 is not q1
    assert f1 not in q2.features
    assert f2 in q2.features
    assert len(q2.features) == 1

    try:
        _ = q1 - f3
        assert False
    except ValueError as _:
        assert True
    except Exception as _:
        assert False

    q2 += f1

    assert f1 in q2.features
    assert f2 in q2.features
    assert len(q2.features) == 2

    qf = f1 == "string"

    assert qf.feature == f1
    assert qf.feature != f2
    assert Operations[qf.operation] == Operations.OBJ_LIKE
    assert isinstance(qf, QueryFilter)
