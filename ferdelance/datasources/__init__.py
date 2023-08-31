__all__ = [
    "DataSource",
    "DataSourceDB",
    "DataSourceFile",
    "DataSourceStorage",
]

from .datasource import DataSource
from .dbs import DataSourceDB
from .files import DataSourceFile

from ferdelance.config import DataSourceConfiguration, get_logger
from ferdelance.schemas.metadata import Metadata


LOGGER = get_logger(__name__)


class DataSourceStorage:
    def __init__(self, datasources: list[DataSourceConfiguration]) -> None:
        """Hash -> DataSource"""
        self.datasources: dict[str, DataSourceDB | DataSourceFile] = dict()

        for ds in datasources:
            if ds.token is None:
                tokens = list()
            elif isinstance(ds.token, str):
                tokens = [ds.token]
            else:
                tokens = ds.token

            if ds.kind == "db":
                if ds.conn is None:
                    LOGGER.error(f"Missing connection for datasource with name={ds.conn}")
                    continue
                datasource = DataSourceDB(ds.name, ds.type, ds.conn, tokens)
                self.datasources[datasource.hash] = datasource

            if ds.kind == "file":
                if ds.path is None:
                    LOGGER.error(f"Missing path for datasource with name={ds.conn}")
                    continue
                datasource = DataSourceFile(ds.name, ds.type, ds.path, tokens)
                self.datasources[datasource.hash] = datasource

    def metadata(self) -> Metadata:
        return Metadata(datasources=[ds.metadata() for _, ds in self.datasources.items()])
