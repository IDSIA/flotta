from __future__ import annotations

from ..schema.workbench import *


class Filter:

    def __init__(self, feature: Feature, operation: str, parameter: int | float | str) -> None:
        # TODO: operations should be limited (i.e. <,>,=>,<=,==,!=)
        self.feature = feature
        self.parameter = parameter
        self.operation = operation


class Transformer:

    def __init__(self, feature: Feature, name: str, parameters: dict[str, str]) -> None:
        # TODO: parameters should be sanitized
        self.feature = feature
        self.name = name
        self.parameters = parameters


class Query:

    def __init__(self) -> None:
        self.datasources: set[int] = set()
        self.features: list[Feature] = list()

        self.filters: list[Filter] = list()
        self.transformers: list[Transformer] = list()

    def add_datasource(self, ds: DataSourceDetails) -> Query:
        for f in ds.features:
            self.add_feature(f)
        return self

    def add_feature(self, f: Feature) -> Query:
        self.datasources.update(f.datasource_id)
        self.features.append(f)
        return self

    def remove_feature(self, f: Feature) -> Query:
        self.features.remove(f)
        if not any(f.datasource_id == f.datasource_id for f in self.features):
            self.datasources.remove(f.datasource_id)
        return self

    def filter(self, f: Feature, op: str, p: int | float | str) -> Query:
        # TODO: save the filter to be applied on the given feature
        # TODO: do we want to allow complex (i.e. OR) filter?
        self.filters.append(Filter(f, op, p))
        return self

    def transform(self, f: Feature, transformer: str, params: dict[str, str]) -> Query:
        # TODO: accept a series of possible transformer like:
        #       - discretization
        #       - scaler
        #       - inputation
        self.transformers.append(Transformer(f, transformer, params))
        return self
