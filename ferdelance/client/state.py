from ferdelance import __version__
from ferdelance.config import Configuration, DataSourceConfiguration, DataSourceStorage
from ferdelance.logging import get_logger

import os
import yaml


LOGGER = get_logger(__name__)


class State:
    def __init__(self, config: Configuration, leave: bool = False) -> None:
        self.config: Configuration = config

        self.leave: bool = leave

        self.client_id: str = ""
        self.remote_public_key: str | None = None

        self.datasources: list[DataSourceConfiguration] = config.datasources

        LOGGER.debug(f"datasources found: {len(self.datasources)}")

        self.data: DataSourceStorage = DataSourceStorage(config.datasources)

        # TODO: re-enable this check
        # if not self.data.datasources:
        #     LOGGER.error("No valid datasource available!")
        #     raise ConfigError()

    def join(self, client_id: str, remote_public_key: str) -> None:
        self.client_id = client_id
        self.remote_public_key = remote_public_key

        LOGGER.info(f"assigned client_id={self.client_id}")

        self.dump_props()

    def get_server(self) -> str:
        return self.config.node.url_extern()

    def path_properties(self) -> str:
        return os.path.join(self.config.workdir, "properties.yaml")

    def read_props(self):
        with open(self.path_properties(), "r") as f:
            props_data = yaml.safe_load(f)

            props = props_data["client"]

            self.client_id = props["client_id"]
            self.remote_public_key = props["remote_public_key"]

    def dump_props(self):
        """Save current configuration to a file in the working directory."""
        with open(self.path_properties(), "w") as f:
            yaml.safe_dump(
                {
                    "client": {
                        "version": __version__,
                        "client_id": self.client_id,
                        "remote_public_key": self.remote_public_key,
                    },
                },
                f,
            )
