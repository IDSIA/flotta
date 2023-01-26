from .datasource import DataSource

import pandas as pd


class DataSourceDB(DataSource):
    def __init__(self, datasource_id: str, name: str, type: str, connection_string: str, token: str = "") -> None:
        super().__init__(datasource_id, name, type, token)
        self.connection_string: str = connection_string

    def get(self) -> pd.DataFrame:
        # TODO open connection, filter content, pack as pandas DF
        raise NotImplementedError()

    def dump(self) -> dict[str, str]:
        return super().dump() | {
            "conn": self.connection_string,
        }
