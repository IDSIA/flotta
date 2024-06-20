from flotta.datasources.datasource import DataSource

import pandas as pd


class DataSourceDB(DataSource):
    def __init__(
        self,
        name: str,
        type: str,
        connection_string: str,
        tokens: list[str] = list(),
        encoding: str = "utf8",
    ) -> None:
        super().__init__(name, type, connection_string, tokens, encoding)
        self.connection_string: str = connection_string

    def get(self) -> pd.DataFrame:
        # TODO open connection, filter content, pack as pandas DF
        raise NotImplementedError()

    def dump(self) -> dict[str, str]:
        return super().dump() | {
            "conn": self.connection_string,
        }
