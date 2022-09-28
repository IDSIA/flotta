from ferdelance_shared.schemas import DataSource as BaseDataSource

from .features import Feature


class DataSource(BaseDataSource):
    features: list[Feature]
