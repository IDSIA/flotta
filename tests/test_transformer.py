from ferdelance_shared.transformers import Transformer
from ferdelance_shared.artifacts import QueryTransformer, QueryFeature


class TestTransformer:

    def test_ft_build(self):
        ft = Transformer('example', 'Feature1', 'Latitude2')
        qt: QueryTransformer = ft.build()

        assert qt.name == 'example'
        assert qt.features_in == ft.features_in
        assert qt.features_out == ft.features_out
        assert len(qt.parameters) == 0

    def test_ft_inputs(self):
        try:
            Transformer('example1', 'Feature1', ['wrong1', 'wrong2'])
            assert False
        except ValueError:
            assert True

        try:
            Transformer('example2', ['f1', 'f2'], ['wrong1', 'wrong2'])
            assert True
        except ValueError:
            assert False

        qf1 = QueryFeature(feature_id='1', datasource_id='1', feature_name='feature_1', datasource_name='ds1')
        qf2 = QueryFeature(feature_id='2', datasource_id='2', feature_name='feature_2', datasource_name='ds2')

        try:
            Transformer('example2', qf1, ['wrong1', 'wrong2'])
            assert False
        except ValueError:
            assert True

        try:
            t = Transformer('example2', [qf1, qf2], ['wrong1', 'wrong2'])

            assert True
            assert len(t.features_in) == 2
            assert len(t.features_out) == 2
            assert qf1.feature_name in t.features_in
            assert qf2.feature_name in t.features_in
            assert 'wrong1' in t.features_out
            assert 'wrong2' in t.features_out
        except ValueError:
            assert False
