from __future__ import annotations
from typing import Any
from unicodedata import name

from ..schema.workbench import *


class QueryFeature:

    def __init__(self, feature_id: str, datasource_id: int) -> None:
        self.feature_id: str = feature_id
        self.datasource_id: int = datasource_id

    def json(self) -> dict[str, str]:
        return {
            'feature_id': self.feature_id,
            'datasource_id': self.datasource_id,
        }


class Filter:

    def __init__(self, feature: QueryFeature, operation: str, parameter: int | float | str) -> None:
        # TODO: operations should be limited (i.e. <,>,=>,<=,==,!=)
        self.feature: QueryFeature = feature
        self.operation: str = operation
        self.parameter: str = parameter

    def json(self) -> dict[str, str]:
        return {
            'feature': self.feature.json(),
            'operation': self.operation,
            'parameter': self.parameter,
        }


class Transformer:

    def __init__(self, feature: QueryFeature, name: str, parameters: dict[str, str]) -> None:
        # TODO: parameters should be sanitized
        self.feature: QueryFeature = feature
        self.name: str = name
        self.parameters: str = parameters

    def json(self) -> dict[str, str]:
        return {
            'feature': self.feature.json(),
            'name': self.name,
            'parameters': self.parameters,
        }


class Query:

    def __init__(self) -> None:
        self.datasources: set[int] = set()
        self.features: list[QueryFeature] = list()

        self.filters: list[Filter] = list()
        self.transformers: list[Transformer] = list()

    def add_datasource(self, ds: DataSourceDetails) -> Query:
        for f in ds.features:
            self.add_feature(f)
        return self

    def add_feature(self, f: Feature) -> Query:
        qf = QueryFeature(f.feature_id, f.datasource_id)
        self.datasources.add(qf.datasource_id)
        self.features.append(qf)
        return self

    def remove_feature(self, f: Feature) -> Query:
        qf = QueryFeature(f.feature_id, f.datasource_id)
        self.features.remove(qf)
        if not any(f.datasource_id == f.datasource_id for f in self.features):
            self.datasources.remove(f.datasource_id)
        return self

    def filter(self, f: Feature, op: str, p: int | float | str) -> Query:
        # TODO: save the filter to be applied on the given feature
        # TODO: do we want to allow complex (i.e. OR) filter?
        qf = QueryFeature(f.feature_id, f.datasource_id)
        self.filters.append(Filter(qf, op, p))
        return self

    def transform(self, f: Feature, transformer: str, params: dict[str, str]) -> Query:
        # TODO: accept a series of possible transformer like:
        #       - discretization
        #       - scaler
        #       - inputation
        qf = QueryFeature(f.feature_id, f.datasource_id)
        self.transformers.append(Transformer(qf, transformer, params))
        return self

    def json(self) -> dict[str, Any]:
        return {
            'datasources': list(self.datasources),
            'features': [f.json() for f in self.features],
            'filters': [f.json() for f in self.filters],
            'transformers': [t.json() for t in self.transformers],
        }

    def __str__(self) -> str:
        # TODO: enhance
        return f'Query'

    def __repr__(self) -> str:
        return self.__str__()


def feature_from_json(data: dict[str, Any]) -> QueryFeature:
    return QueryFeature(feature_id=data['feature_id'], datasource_id=data['datasource_id'])


def filter_from_json(data: dict[str, Any]) -> Filter:
    qf = feature_from_json(data['feature'])
    return Filter(qf, data['operation'], data['parameter'])


def transformer_from_json(data: dict[str, Any]) -> Transformer:
    qf = feature_from_json(data['feature'])
    return Transformer(qf, data['name'], data['parameters'])


def query_from_json(data: dict[str, Any]) -> Query:
    q = Query()

    q.datasources.update(data['datasources'])

    features: dict[str, QueryFeature] = dict()

    for f in data['features']:
        qf = feature_from_json(f)
        q.features.append(qf)
        features[f'{qf.feature_id}_{qf.datasource_id}'] = qf

    for f in data['filters']:
        qf = features[f"{f['feature']['feature_id']}_{f['feature']['datasource_id']}"]
        q.filters.append(Filter(qf, f['operation'], f['parameter']))

    for t in data['transformers']:
        qf = features[f"{t['feature']['feature_id']}_{t['feature']['datasource_id']}"]
        q.transformers.append(Transformer(qf, t['name'], t['parameters']))

    return q
