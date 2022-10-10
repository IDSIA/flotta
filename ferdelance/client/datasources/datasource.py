from ferdelance_shared.schemas import MetaDataSource

import pandas as pd


class DataSource:
    def __init__(self, name: str, kind: str) -> None:
        self.datasource_id: str | None = None
        self.name: str = name
        self.kind: str = kind

    def get(self) -> pd.DataFrame:
        raise NotImplementedError()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self)) and self.name == other.name and self.kind == other.kind

    def __hash__(self) -> int:
        return hash((self.name, self.kind))

    def __str__(self) -> str:
        return f'({self.kind}) {self.name}'

    def __repr__(self) -> str:
        return f'({self.kind}) {self.name}'

    def metadata(self) -> MetaDataSource:
        raise NotImplementedError()
