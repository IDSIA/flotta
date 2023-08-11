from ferdelance.schemas.queries import QueryTransformer, QueryFeature
from ferdelance.schemas.transformers import Transformer


def test_ft_build():
    f_in = QueryFeature(name="Feature1", dtype="str")

    ft = Transformer("example", f_in, "Latitude2")
    qt: QueryTransformer = ft.build()

    assert qt.name == "example"
    assert len(qt.features_in) == 1
    assert len(qt.features_out) == 1

    qt_f_in = qt.features_in[0]
    qt_f_out = qt.features_out[0]

    assert qt_f_in == f_in
    assert qt_f_out.name == "Latitude2"

    assert len(qt.parameters) == 0


def test_ft_inputs():
    qf1 = QueryFeature(name="feature_1", dtype="int")
    qf2 = QueryFeature(name="feature_2", dtype="int")

    try:
        Transformer("example1", qf1, ["wrong1", "wrong2"])
        assert False
    except ValueError:
        assert True

    try:
        Transformer("example2", [qf1, qf2], ["wrong1", "wrong2"])
        assert True
    except ValueError:
        assert False

    try:
        t = Transformer("example3", [qf1, qf2], ["wrong1", "wrong2"])

        assert True
        assert len(t.features_in) == 2
        assert len(t.features_out) == 2
        assert qf1.name in t._columns_in
        assert qf2.name in t._columns_in
        assert "wrong1" in t._columns_out
        assert "wrong2" in t._columns_out
    except ValueError:
        assert False
