from typing import Any

from ferdelance.const import TYPE_CLIENT, TYPE_NODE
from ferdelance.datasources import DataSourceDB, DataSourceFile
from ferdelance.logging import get_logger
from ferdelance.schemas.metadata import Metadata
from ferdelance.shared.exchange import Exchange

from .arguments import setup_config_from_arguments

from pydantic import BaseSettings, BaseModel, root_validator, validator
from dotenv import load_dotenv

from pathlib import Path

import os
import re
import yaml


load_dotenv()

ENV_VAR_PATTERN = re.compile(r".*?\${(\w+)}.*?")

LOGGER = get_logger(__name__)


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


def clean_protocol_port(protocol: str, port: int) -> tuple[str, str]:
    _protocol, _port = protocol, f":{port}"

    if port == 80:
        _protocol, _port = "http", ""
    if port == 443:
        _protocol, _port = "https", ""

    if not _protocol:
        _protocol = "http"

    return _protocol, _port


class NodeConfiguration(BaseModel):
    name: str = ""

    main_password: str = ""

    # protocol used
    protocol: str = "http"
    # interface to use
    interface: str = "0.0.0.0"
    # external host (or fqdn) to use
    url: str = "localhost"
    # external port to listen to
    port: int = 1456

    token_project_default: str = ""

    # self-check in seconds when mode=node
    healthcheck: float = 60
    # concat server node each interval in second for update when mode=client
    heartbeat: float = 2.0

    @root_validator(pre=True)
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        return check_for_env_variables(values, "ferdelance_node")


class JoinConfiguration(BaseModel):
    first: bool = False
    url: str | None = None

    @root_validator(pre=True)
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        return check_for_env_variables(values, "ferdelance_client")


class DatabaseConfiguration(BaseModel):
    username: str | None = None
    password: str | None = None

    dialect: str = "sqlite"  # "postgresql"
    port: int = -1  # 5432
    host: str | None = "./storage/sqlite.db"  # None
    scheme: str = "ferdelance"

    memory: bool = False

    @root_validator(pre=True)
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        return check_for_env_variables(values, "ferdelance_database")


class DataSourceConfiguration(BaseModel):
    name: str
    token: list[str] | str | None
    kind: str
    type: str
    conn: str | None = None
    path: str | None = None

    @root_validator(pre=True)
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        return check_for_env_variables(values, "ferdelance_datasource")


class DataSourceStorage:
    def __init__(self, datasources: list[DataSourceConfiguration]) -> None:
        """Hash -> DataSource"""
        self.datasources: dict[str, DataSourceDB | DataSourceFile] = dict()
        self.ds_configs: list[DataSourceConfiguration] = datasources

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

    def hashes(self) -> list[str]:
        return list(self.datasources.keys())

    def __getitem__(self, key: str) -> DataSourceDB | DataSourceFile | None:
        return self.datasources.get(key, None)

    def __len__(self) -> int:
        return len(self.datasources)


class Configuration(BaseSettings):
    database: DatabaseConfiguration = DatabaseConfiguration()

    node: NodeConfiguration = NodeConfiguration()
    join: JoinConfiguration = JoinConfiguration()

    datasources: list[DataSourceConfiguration] = list()

    mode: str = "node"

    workdir: str = os.path.join(".", "storage")

    file_chunk_size: int = 4096

    @validator("mode")
    @classmethod
    def mode_validator(cls, v, values, **kwargs):
        valid_modes = [
            "client",
            "node",
            "standalone",
        ]

        # check for valid mode
        if v not in valid_modes:
            raise ValueError(f"Invalid mode: expected one of {[valid_modes]}")

        # check for existing join url
        if v == "client":
            os.environ["FERDELANCE_MODE"] = "client"

            j: JoinConfiguration = values["join"]

            if j.first or not j.url:
                raise ValueError("No join node set!")

        return v

    @root_validator(pre=True)
    @classmethod
    def env_var_validate(cls, values: dict[str, Any]):
        values = check_for_env_variables(values, "ferdelance")

        # Force node url to localhost when mode=client
        if "mode" in values and values["mode"] == "client":
            LOGGER.info("client mode detected, forcing api to localhost")
            node = values["node"]
            node["protocol"] = "http"
            node["interface"] = "localhost"
            node["port"] = 1456

        return values

    def get_node_type(self) -> str:
        if self.mode == "client":
            return TYPE_CLIENT
        if self.mode == "node":
            return TYPE_NODE
        if self.mode == "standalone":
            return TYPE_NODE

        raise ValueError(f"invalid or unsupported mode={self.mode}")

    def url_extern(self) -> str:
        """Url that will be sent to other nodes and used to contact this node."""
        _protocol, _port = clean_protocol_port(self.node.protocol, self.node.port)

        return f"{_protocol}://{self.node.url.rstrip('/')}{_port}"

    def url_deploy(self) -> str:
        """Url to use for deploying the api through ray serve."""
        _protocol, _port = clean_protocol_port(self.node.protocol, self.node.port)

        return f"{_protocol}://{self.node.interface.rstrip('/')}{_port}"

    def get_workdir(self) -> Path:
        return Path(self.workdir)

    def storage_datasources_dir(self) -> Path:
        return self.get_workdir() / "datasources"

    def storage_datasources(self, datasource_hash: str) -> Path:
        return self.storage_datasources_dir() / datasource_hash

    def storage_artifact_dir(self) -> Path:
        return self.get_workdir() / "artifacts"

    def storage_artifact(self, artifact_id: str, iteration: int = 0) -> Path:
        return self.storage_artifact_dir() / artifact_id / str(iteration)

    def storage_job(self, artifact_id: str, job_id: str, iteration: int = 0) -> Path:
        d = self.storage_artifact(artifact_id, iteration) / job_id
        os.makedirs(d, exist_ok=True)
        return d

    def storage_clients_dir(self) -> Path:
        return self.get_workdir() / "clients"

    def storage_clients(self, client_id: str) -> Path:
        return self.storage_clients_dir() / client_id

    def storage_config(self) -> Path:
        return self.get_workdir() / "config.yaml"

    def private_key_location(self) -> Path:
        return self.get_workdir() / "private_key.pem"

    def storage_properties(self) -> Path:
        return self.get_workdir() / "properties.yaml"

    def dump(self) -> None:
        os.makedirs(self.get_workdir(), exist_ok=True)
        with open(self.storage_config(), "w") as f:
            try:
                yaml.safe_dump(self.dict(), f)

                os.environ["FERDELANCE_CONFIG_FILE"] = str(self.storage_config())

            except yaml.YAMLError as e:
                LOGGER.error(f"could not dump config file to {self.storage_config()}")
                LOGGER.exception(e)

    class Config:
        env_prefix = "ferdelance_"
        env_nested_delimiter = "_"


class ConfigManager:
    def __init__(self) -> None:
        self.config: Configuration
        self._leave: bool = False

        self._set_config()

        self.data: DataSourceStorage = DataSourceStorage(self.config.datasources)

    def _set_config(self) -> None:
        # config path from cli parameters
        config_path, self._leave = setup_config_from_arguments()

        # config path from env variable
        env_path = os.environ.get("FERDELANCE_CONFIG_FILE", None)

        if env_path is not None:
            config_path = Path(env_path)
            LOGGER.info(f"configuration file provided through environment variable path={config_path}")

        # default config path
        if config_path is None:
            LOGGER.info("no configuration file provided")
            self._set_default_config()
            return

        if not os.path.exists(config_path):
            LOGGER.warn(f"configuration file not found at {config_path}")
            self._set_default_config()
            return

        LOGGER.info(f"loading configuration from path={config_path}")

        with open(config_path, "r") as f:
            try:
                yaml_data: dict[str, Any] = yaml.safe_load(f)
                self.config = Configuration(**yaml_data)

            except yaml.YAMLError as e:
                LOGGER.error(f"could not read config file {config_path}")
                LOGGER.exception(e)
                self._set_default_config()

    def _set_default_config(self) -> None:
        LOGGER.warning("using default configuration.")
        self.config: Configuration = Configuration()
        self.config.dump()

    def _set_keys(self) -> None:
        exc = Exchange()

        path_private_key = self.config.private_key_location()

        if os.path.exists(path_private_key):
            # use existing one
            LOGGER.info(f"private key found at {path_private_key}")
            exc.load_key(path_private_key)

        else:
            # generate new key
            LOGGER.info("private key location not found: creating a new one")

            exc.generate_key()
            exc.save_private_key(path_private_key)

    def _set_directories(self) -> None:
        LOGGER.info("directory initialization")

        # create required directories
        os.makedirs(self.config.storage_artifact_dir(), exist_ok=True)
        os.makedirs(self.config.storage_clients_dir(), exist_ok=True)
        # os.chmod(self.config.workdir, 0o700)

        LOGGER.info("directory initialization completed")

    def setup(self) -> None:
        self._set_config()
        self._set_directories()
        self._set_keys()

    def get(self) -> Configuration:
        return self.config

    def leave(self) -> bool:
        return self._leave


config_manager = ConfigManager()
