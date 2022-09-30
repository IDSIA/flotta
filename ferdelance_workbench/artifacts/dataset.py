from __future__ import annotations
from ferdelance_shared.schemas import (
    Dataset as BaseDataset,
)
from .queries import Query


class Dataset(BaseDataset):
    queries: list[Query] = list()

    def add_query(self, query: Query) -> None:
        self.queries.append(query)

    def copy(self) -> Dataset:
        return Dataset(
            queries=[q.copy() for q in self.queries],
            test_percentage=self.test_percentage,
            val_percentage=self.val_percentage,
            random_seed=self.random_seed,
            label=self.label,
        )

    def __add__(self, other: Query) -> Dataset:
        if isinstance(other, Query):
            d = self.copy()
            d.add_query(other)
            return d

        raise ValueError('Cannot add something that is not a Query')
