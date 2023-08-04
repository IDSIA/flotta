from ferdelance import __version__
from ferdelance.config import Configuration, DataSourceConfiguration
from ferdelance.client.datasources import DataSourceFile, DataSourceDB
from ferdelance.client.exceptions import ConfigError
from ferdelance.schemas.metadata import Metadata

from getmac import get_mac_address

import logging
import os
import platform
import uuid
import yaml


LOGGER = logging.getLogger(__name__)


class DataConfig:
    def __init__(self, workdir: str, datasources: list[DataSourceConfiguration]) -> None:
        self.workdir: str = workdir

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

    def path_artifacts_folder(self) -> str:
        path = os.path.join(self.workdir, "artifacts")
        os.makedirs(path, exist_ok=True)
        return path

    def metadata(self) -> Metadata:
        return Metadata(datasources=[ds.metadata() for _, ds in self.datasources.items()])


class ClientState:
    def __init__(self, config: Configuration, leave: bool = False) -> None:
        self.config: Configuration = config

        self.name: str = config.client.name
        self.server: str = config.server.url()
        self.heartbeat: float = config.client.heartbeat
        self.workdir: str = config.workdir
        self.private_key_location: str = config.private_key_location

        self.leave: bool = leave

        self.machine_system: str = platform.system()
        self.machine_mac_address: str = get_mac_address() or ""
        self.machine_node: str = str(uuid.getnode())

        self.client_id: str | None = None
        self.client_token: str | None = None
        self.server_public_key: str | None = None

        self.datasources: list[DataSourceConfiguration] = config.datasources

        self.data = DataConfig(self.workdir, config.datasources)

        if not self.data.datasources:
            LOGGER.error("No valid datasource available!")
            raise ConfigError()

    def join(self, client_id: str, client_token: str, server_public_key: str) -> None:
        self.client_id = client_id
        self.client_token = client_token
        self.server_public_key = server_public_key

        self.dump_props()

    def set_token(self, client_token: str) -> None:
        self.client_token = client_token

        self.dump_props()

    def get_server(self) -> str:
        return self.server.rstrip("/")

    def path_properties(self) -> str:
        return os.path.join(self.workdir, "properties.yaml")

    def path_private_key(self) -> str:
        if self.private_key_location is None:
            self.private_key_location = os.path.join(self.workdir, "private_key.pem")
        return self.private_key_location

    def read_props(self):
        with open(self.path_properties(), "r") as f:
            props_data = yaml.safe_load(f)

            props = props_data["client"]

            self.client_id = props["client_id"]
            self.client_token = props["client_token"]
            self.server_public_key = props["server_public_key"]

    def dump_props(self):
        """Save current configuration to a file in the working directory."""
        with open(self.path_properties(), "w") as f:
            yaml.safe_dump(
                {
                    "client": {
                        "version": __version__,
                        "client_id": self.client_id,
                        "client_token": self.client_token,
                        "server_public_key": self.server_public_key,
                    },
                },
                f,
            )
