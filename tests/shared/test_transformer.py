from ferdelance.schemas.transformers import Transformer
from ferdelance.schemas.queries import QueryTransformer, QueryFeature


def test_ft_build():
    ft = Transformer("example", "Feature1", "Latitude2")
    qt: QueryTransformer = ft.build()

    assert qt.name == "example"
    assert qt.features_in == ft.features_in
    assert qt.features_out == ft.features_out
    assert len(qt.parameters) == 0


def test_ft_inputs():
    try:
        Transformer("example1", "Feature1", ["wrong1", "wrong2"])
        assert False
    except ValueError:
        assert True

    try:
        Transformer("example2", ["f1", "f2"], ["wrong1", "wrong2"])
        assert True
    except ValueError:
        assert False

    qf1 = QueryFeature(name="feature_1", dtype="int")
    qf2 = QueryFeature(name="feature_2", dtype="int")

    try:
        Transformer("example2", qf1, ["wrong1", "wrong2"])
        assert False
    except ValueError:
        assert True

    try:
        t = Transformer("example2", [qf1, qf2], ["wrong1", "wrong2"])

        assert True
        assert len(t.features_in) == 2
        assert len(t.features_out) == 2
        assert qf1.name in t.features_in
        assert qf2.name in t.features_in
        assert "wrong1" in t.features_out
        assert "wrong2" in t.features_out
    except ValueError:
        assert False
