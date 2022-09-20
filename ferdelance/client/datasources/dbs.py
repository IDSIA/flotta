from .datasource import DataSource

import pandas as pd


class DataSourceDB(DataSource):
    def __init__(self, name: str, kind: str, connection_string: str) -> None:
        super().__init__(name, kind)
        self.connection_string: str = connection_string

    def get(self, label: str = None, filter: str = None) -> pd.DataFrame:
        # TODO open connection, filter content, pack as pandas DF
        raise NotImplementedError()
