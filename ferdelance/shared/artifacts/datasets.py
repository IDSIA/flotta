from __future__ import annotations
from pydantic import BaseModel
from .queries import Query


class Dataset(BaseModel):
    """Query split the data in train/test/validation."""
    queries: list[Query] = list()
    test_percentage: float = 0.0
    val_percentage: float = 0.0
    random_seed: float | None = None
    label: str | None = None

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
