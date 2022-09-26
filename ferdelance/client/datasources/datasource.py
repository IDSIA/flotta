import pandas as pd


class DataSource:
    def __init__(self, name: str, kind: str) -> None:
        self.name = name
        self.kind: str = kind

    def get(self, label: str = None, filter: str = None) -> pd.DataFrame:
        raise NotImplemented()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self)) and self.name == other.name and self.kind == other.kind

    def __hash__(self) -> int:
        return hash((self.name, self.kind))

    def __str__(self) -> str:
        return f'({self.kind}) {self.name}'

    def __repr__(self) -> str:
        return f'({self.kind}) {self.name}'

    def metadata(self) -> dict:
        NotImplementedError()
