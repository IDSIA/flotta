from ferdelance import __version__
from ferdelance.client.datasources import DataSourceFile, DataSourceDB
from ferdelance.client.exceptions import ConfigError
from ferdelance.schemas.client import ArgumentsConfig
from ferdelance.shared.exchange import Exchange

from getmac import get_mac_address

import logging
import os
import platform
import uuid
import yaml


LOGGER = logging.getLogger(__name__)


class Config:
    def __init__(self, args: ArgumentsConfig) -> None:
        self.name: str = args.name
        self.server: str = args.server.rstrip("/")
        self.heartbeat: float = args.heartbeat
        self.workdir: str = args.workdir
        self.private_key_location: str | None = args.private_key_location

        self.resource_n_slow_thread: int = args.resources.n_slow_thread
        self.resource_n_fast_thread: int = args.resources.n_fast_thread

        self.leave: bool = False

        self.machine_system: str = platform.system()
        self.machine_mac_address: str = get_mac_address() or ""
        self.machine_node: str = str(uuid.getnode())

        self.exc: Exchange = Exchange()

        self.client_id: str | None = None
        self.client_token: str | None = None
        self.server_public_key: str | None = None

        """Hash -> DataSource"""
        self.datasources: dict[str, DataSourceDB | DataSourceFile] = dict()

        for ds in args.datasources:
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
                self.datasources[datasource.datasource_hash] = datasource

            if ds.kind == "file":
                if ds.path is None:
                    LOGGER.error(f"Missing path for datasource with name={ds.conn}")
                    continue
                datasource = DataSourceFile(ds.name, ds.type, ds.path, tokens)
                self.datasources[datasource.datasource_hash] = datasource

        if not self.datasources:
            LOGGER.error("No valid datasource available!")
            raise ConfigError()

    def join(self, client_id: str, client_token: str, server_public_key: str) -> None:
        self.client_id = client_id
        self.client_token = client_token
        self.server_public_key = server_public_key

        self.exc.set_token(client_token)
        self.exc.set_remote_key(server_public_key)

        self.dump_props()

    def get_server(self) -> str:
        return self.server.rstrip("/")

    def path_properties(self) -> str:
        return os.path.join(self.workdir, "properties.yaml")

    def path_private_key(self) -> str:
        if self.private_key_location is None:
            return os.path.join(self.workdir, "private_key.pem")
        return self.private_key_location

    def path_artifact_folder(self) -> str:
        path = os.path.join(self.workdir, "artifacts")
        os.makedirs(path, exist_ok=True)
        return path

    def read_props(self):
        with open(self.path_properties(), "r") as f:
            props_data = yaml.safe_load(f)

            props = props_data["ferdelance"]["extra"]

            self.client_id = props["client_id"]
            self.client_token = props["client_token"]
            self.server_public_key = props["server_public_key"]

            self.exc.set_token(self.client_token)
            self.exc.set_remote_key(self.server_public_key)

    def dump_props(self):
        """Save current configuration to a file in the working directory."""
        with open(self.path_properties(), "w") as f:
            yaml.safe_dump(
                {
                    "ferdelance": {
                        "extra": {
                            "version": __version__,
                            "client_id": self.client_id,
                            "client_token": self.client_token,
                            "server_public_key": self.server_public_key,
                        },
                    },
                },
                f,
            )
