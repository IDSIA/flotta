from .datasource import DataSource

import pandas as pd


class DataSourceFile(DataSource):
    def __init__(self, name: str, kind: str, path: str) -> None:
        super().__init__(name, kind)
        self.path: str = path

    def get(self, label: str = None, filter: str = None) -> pd.DataFrame:
        # TODO open file, read content, filter content, pack as pandas DF
        raise NotImplemented()
