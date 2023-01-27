from ferdelance.shared.artifacts import MetaDataSource

import pandas as pd


class DataSource:
    def __init__(self, datasource_id: str, name: str, type: str, token: str = "") -> None:
        self.datasource_id: str = datasource_id
        self.name: str = name
        self.type: str = type
        self.token: str = token

    def get(self) -> pd.DataFrame:
        raise NotImplementedError()

    def dump(self) -> dict[str, str]:
        return {
            "name": self.name,
            "type": self.type,
            "token": self.token,
        }

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, type(self))
            and self.name == other.name
            and self.type == other.type
            and self.token == other.token
        )

    def __hash__(self) -> int:
        return hash((self.name, self.type, self.token))

    def __str__(self) -> str:
        return f"({self.type}) {self.name}"

    def __repr__(self) -> str:
        return f"({self.type}) {self.name}"

    def metadata(self) -> MetaDataSource:
        raise NotImplementedError()
