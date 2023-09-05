from ferdelance import __version__
from ferdelance.config import Configuration, DataSourceConfiguration, DataSourceStorage
from ferdelance.client.exceptions import ConfigError
from ferdelance.logging import get_logger

import os
import yaml


LOGGER = get_logger(__name__)


class State:
    def __init__(self, config: Configuration, leave: bool = False) -> None:
        self.config: Configuration = config

        self.name: str = config.node.name
        self.server: str = config.node.url_extern()
        self.heartbeat: float = config.node.heartbeat
        self.workdir: str = config.workdir
        self.private_key_location: str = config.private_key_location()

        self.leave: bool = leave

        self.machine_system: str = config.node.machine_system
        self.machine_mac_address: str = config.node.machine_mac_address
        self.machine_node: str = config.node.machine_node

        LOGGER.debug(
            f"machine data: system={self.machine_system} "
            f"mac_address={self.machine_mac_address} "
            f"node={self.machine_node}"
        )

        self.client_id: str | None = None
        self.client_token: str | None = None
        self.node_public_key: str | None = None

        self.datasources: list[DataSourceConfiguration] = config.datasources

        LOGGER.debug(f"datasources found: {len(self.datasources)}")

        self.data: DataSourceStorage = DataSourceStorage(config.datasources)

        # TODO: re-enable this check
        # if not self.data.datasources:
        #     LOGGER.error("No valid datasource available!")
        #     raise ConfigError()

    def join(self, client_id: str, client_token: str, node_public_key: str) -> None:
        self.client_id = client_id
        self.client_token = client_token
        self.node_public_key = node_public_key

        LOGGER.info(f"assigned client_id={self.client_id}")
        LOGGER.debug(f"assigned client_token={self.client_token}")

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
            self.node_public_key = props["node_public_key"]

    def dump_props(self):
        """Save current configuration to a file in the working directory."""
        with open(self.path_properties(), "w") as f:
            yaml.safe_dump(
                {
                    "client": {
                        "version": __version__,
                        "client_id": self.client_id,
                        "client_token": self.client_token,
                        "node_public_key": self.node_public_key,
                    },
                },
                f,
            )
