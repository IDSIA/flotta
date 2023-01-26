from ferdelance import __version__
from ferdelance.client.datasources import DataSourceFile, DataSourceDB
from ferdelance.shared.exchange import Exchange

from pydantic import BaseModel

import logging
import os
import yaml


LOGGER = logging.getLogger(__name__)


class ConfigError(Exception):
    def __init__(self, *args: str) -> None:
        self.what_is_missing: list[str] = list(args)


class Config(BaseModel):

    server: str = "http://localhost/"
    heartbeat: float = 1.0

    workdir: str = "./workdir"
    private_key_location: str | None = None

    leave: bool = False

    client_id: str | None = None
    client_token: str | None = None
    server_public_key: str | None = None

    datasources_list: list[DataSourceFile | DataSourceDB] = list()
    datasources_by_id: dict[str, DataSourceFile | DataSourceDB] = dict()

    exc: Exchange = Exchange()

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

    def add_datasource(
        self, datasource_id: str, kind: str, name: str, type: str, conn: str, path: str, token: str
    ) -> None:
        if kind == "db":
            self.datasources_list.append(DataSourceDB(datasource_id, name, type, conn, token))
        if kind == "file":
            self.datasources_list.append(DataSourceFile(datasource_id, name, type, path, token))

    def join(self, client_id: str, client_token: str, server_public_key: str) -> None:
        self.client_id = client_id
        self.client_token = client_token
        self.server_public_key = server_public_key

        self.exc.set_token(client_token)
        self.exc.set_remote_key(server_public_key)

        self.dump_props()

    def read_props(self):
        with open(self.path_properties(), "r") as f:
            props_data = yaml.safe_load(f)

            props = props_data["ferdelance"]["extra"]

            self.client_id = props["client_id"]
            self.client_token = props["client_token"]
            self.server_public_key = props["server_public_key"]

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
