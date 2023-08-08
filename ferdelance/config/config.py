from typing import Any, Literal

from pydantic import BaseSettings, BaseModel, root_validator

from .arguments import setup_config_from_arguments
from .logging import LOGGING_CONFIG

from dotenv import load_dotenv
from getmac import get_mac_address

import logging.config
import logging
import os
import platform
import re
import uuid
import yaml


load_dotenv()

ENV_VAR_PATTERN = re.compile(r".*?\${(\w+)}.*?")

logging.config.dictConfig(LOGGING_CONFIG)

LOGGER = logging.getLogger(__name__)


def check_for_env_variables(value_in: dict[str, Any], prefix: str) -> dict[str, Any]:
    """Source: https://dev.to/mkaranasou/python-yaml-configuration-with-environment-variables-parsing-2ha6"""

    value_out = dict()

    for key, value in value_in.items():
        value_out[key] = value

        if isinstance(value, list):
            continue

        if isinstance(value, dict):
            value_out[key] = check_for_env_variables(value, f"{prefix}_{key}")
            continue

        var_env = f"{prefix}_{key}".upper()
        value = os.environ.get(var_env, value)
        LOGGER.debug(f"Configuration: {key:24} {var_env:48} {value}")

        value_out[key] = value

        if isinstance(value, str):
            # find all env variables in line
            match = ENV_VAR_PATTERN.findall(value)

            # TODO: testing required

            if match:
                full_value: str = value
                for g in match:
                    full_value = full_value.replace(f"${{{g}}}", os.environ.get(g, g))
                value_out[key] = full_value

    return value_out


class ServerConfiguration(BaseModel):
    main_password: str = ""

    protocol: str = "http"
    interface: str = "0.0.0.0"
    port: int = 1456

    token_client_expiration: str = "90 days"
    token_user_expiration: str = "30 days"
    token_project_default: str = ""

    healthcheck: int = 60

    @root_validator()
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        return check_for_env_variables(values, "ferdelance_server")

    def url(self) -> str:
        return f"{self.protocol}://{self.interface.rstrip('/')}:{self.port}"


class DatabaseConfiguration(BaseModel):
    username: str | None = None
    password: str | None = None

    dialect: str = "postgresql"
    port: int = 5432
    host: str | None = None
    scheme: str = "ferdelance"

    memory: bool = False

    @root_validator(pre=True)
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        return check_for_env_variables(values, "ferdelance_database")


class ClientConfiguration(BaseModel):
    heartbeat: float = 2.0

    name: str = ""

    machine_system: str = platform.system()
    machine_mac_address: str = get_mac_address() or ""
    machine_node: str = str(uuid.getnode())

    @root_validator()
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        return check_for_env_variables(values, "ferdelance_client")


class DataSourceConfiguration(BaseModel):
    name: str
    token: list[str] | str | None
    kind: str
    type: str
    conn: str | None = None
    path: str | None = None

    @root_validator()
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        return check_for_env_variables(values, "ferdelance_datasource")


class Configuration(BaseSettings):
    database: DatabaseConfiguration = DatabaseConfiguration()

    server: ServerConfiguration = ServerConfiguration()
    client: ClientConfiguration = ClientConfiguration()

    datasources: list[DataSourceConfiguration] = list()

    mode: Literal["client", "server", "standalone", "distributed"] = "standalone"

    workdir: str = os.path.join(".", "storage")
    private_key_location: str = os.path.join(".", "private_key.pem")

    file_chunk_size: int = 4096

    @root_validator()
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        return check_for_env_variables(values, "ferdelance")

    def storage_datasources_dir(self) -> str:
        return os.path.join(self.workdir, "datasources")

    def storage_datasources(self, datasource_hash: str) -> str:
        return os.path.join(self.storage_datasources_dir(), datasource_hash)

    def storage_artifact_dir(self) -> str:
        return os.path.join(self.workdir, "artifacts")

    def storage_artifact(self, artifact_id: str, iteration: int = 0) -> str:
        return os.path.join(self.storage_artifact_dir(), artifact_id, str(iteration))

    def storage_clients_dir(self) -> str:
        return os.path.join(self.workdir, "clients")

    def storage_clients(self, client_id: str) -> str:
        return os.path.join(self.storage_clients_dir(), client_id)

    def storage_results_dir(self) -> str:
        return os.path.join(self.workdir, "results")

    def storage_results(self, result_id: str) -> str:
        return os.path.join(self.storage_results_dir(), result_id)

    def storage_config(self) -> str:
        return os.path.join(self.workdir, "config.yaml")

    def dump(self) -> None:
        os.makedirs(self.storage_clients_dir(), exist_ok=True)

        with open(self.storage_config(), "w") as f:
            try:
                yaml.safe_dump(self.dict(), f)

                os.environ["FERDELANCE_CONFIG_FILE"] = self.storage_config()

            except yaml.YAMLError as e:
                LOGGER.error(f"could not dump config file to {self.storage_config()}")
                LOGGER.exception(e)

    class Config:
        env_prefix = "ferdelance_"
        env_nested_delimiter = "_"


class ConfigManager:
    def __init__(self) -> None:
        # config path from cli parameters
        config_path, _ = setup_config_from_arguments()

        # config path from env variable
        env_path = os.environ.get("FERDELANCE_CONFIG_FILE", "")

        if env_path:
            config_path: str = env_path
            LOGGER.info(f"Configuration file provided through environment variable path={config_path}")

        # default config path
        if not config_path:
            LOGGER.info("No configuration file provided, using default parameters")
            self.config: Configuration = Configuration()
            self.config.dump()
            return

        if not os.path.exists(config_path):
            LOGGER.warn(f"Configuration file not found at {config_path}, using default parameters")
            self.config: Configuration = Configuration()
            self.config.dump()
            return

        LOGGER.info(f"Loading configuration from path={config_path}")

        with open(config_path, "r") as f:
            try:
                yaml_data: dict[str, Any] = yaml.safe_load(f)
                self.config = Configuration(**yaml_data)

            except yaml.YAMLError as e:
                LOGGER.error(f"could not read config file {config_path}")
                LOGGER.exception(e)
                self.config: Configuration = Configuration()

            self.config.dump()

    def get(self) -> Configuration:
        return self.config


config_manager = ConfigManager()
