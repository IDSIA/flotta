from typing import Any
from ferdelance.core.environment import Environment
from ferdelance.core.queries import QueryFeature
from ferdelance.core.transformers import QueryTransformer


class DummyTransformer(QueryTransformer):
    def transform(self, env: Environment) -> tuple[Environment, Any]:
        return env, None

    def aggregate(self, env: Environment) -> Environment:
        return env


def test_ft_inputs():
    qf_in1 = QueryFeature("feature_1", "int")
    qf_in2 = QueryFeature("feature_2", "int")

    qf_out1 = QueryFeature("wrong1", "int")
    qf_out2 = QueryFeature("wrong2", "int")

    # TODO: currently the transformers have disabled the check for the number of columns. Should it be put back?

    try:
        t = DummyTransformer(features_in=[qf_in1, qf_in2], features_out=[qf_out1, qf_out2])

        assert True
        assert len(t.features_in) == 2
        assert len(t.features_out) == 2
        assert qf_in1.name in t._columns_in()
        assert qf_in2.name in t._columns_in()
        assert "wrong1" in t._columns_out()
        assert "wrong2" in t._columns_out()
    except ValueError:
        assert False
