from __future__ import annotations

from ferdelance_shared.schemas import DataSource as BaseDataSource

from .queries import Query, Feature


class DataSource(BaseDataSource):
    features: list[Feature]

    def all_features(self):
        return Query(
            datasources_id=self.datasource_id,
            features=[f.qf() for f in self.features]
        )

    def __eq__(self, other: DataSource) -> bool:
        if not isinstance(other, DataSource):
            return False

        return self.datasource_id == other.datasource_id

    def __hash__(self) -> int:
        return hash(self.datasource_id)
