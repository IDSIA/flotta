from typing import Literal
from functools import lru_cache

from pydantic import BaseModel, validator, BaseSettings
from pytimeparse import parse

from dotenv import load_dotenv
from getmac import get_mac_address

import os
import platform
import uuid
import yaml


load_dotenv()


class ServerConfiguration(BaseSettings):
    main_password: str = ""

    protocol: str = "http"
    interface: str = "localhost"
    port: int = 1456

    token_client_expiration: int | float | str = "90 days"
    token_user_expiration: int | float | str = "30 days"
    token_project_default: str = ""

    @validator("token_client_expiration", "token_user_expiration", pre=True)
    @classmethod
    def validate_expiration_time(cls, v: str) -> int | float | None:
        return parse(v)

    def server_url(self) -> str:
        return f"{self.protocol}://{self.interface.rstrip('/')}:{self.port}"

    class Config:
        env_prefix = "ferdelance_server_"


class DatabaseConfiguration(BaseModel):
    username: str | None = None
    password: str | None = None

    dialect: str = "postgresql"
    port: int = 5432
    host: str | None = None
    scheme: str = "ferdelance"

    memory: bool = False

    class Config:
        env_prefix = "ferdelance_db_"


class ClientConfiguration(BaseModel):
    heartbeat: float = 2.0

    machine_system: str = platform.system()
    machine_mac_address: str = get_mac_address() or ""
    machine_node: str = str(uuid.getnode())

    class Config:
        env_prefix = "ferdelance_client_"


class DataSourceConfiguration(BaseModel):
    name: str
    kind: str
    type: str = ""
    path: str = ""
    conn: str = ""
    token: list[str]


class Configuration(BaseModel):
    database: DatabaseConfiguration = DatabaseConfiguration()

    server: ServerConfiguration = ServerConfiguration()
    client: ClientConfiguration = ClientConfiguration()

    datasources: list[DataSourceConfiguration] = list()

    mode: Literal["client", "server", "standalone", "distributed"] = "standalone"

    workdir: str = os.path.join(".", "storage")
    private_key_location: str = os.path.join(".", "private_key.pem")

    standalone: bool = False
    distributed: bool = False

    file_chunk_size: int = 4096

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

    class Config:
        env_prefix = "ferdelance_"


@lru_cache
def get_config():
    configuration_file: str = os.environ.get("ferdelance_config_file", "")

    if os.path.exists(configuration_file):
        with open(configuration_file, "r") as f:
            conf = yaml.safe_load(f)
            return Configuration(**conf)

    return Configuration()


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": "%(asctime)s %(levelname)8s %(name)48.48s:%(lineno)-3s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "console_critical": {
            "level": "ERROR",
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "standard",
            "filename": "ferdelance.log",
            "maxBytes": 1024 * 1024 * 1024,  # 1GB
            "backupCount": 5,
        },
        "file_uvicorn_access": {
            "level": "INFO",
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "standard",
            "filename": "ferdelance_access.log",
            "maxBytes": 1024 * 1024 * 1024,  # 1GB
            "backupCount": 5,
        },
    },
    "loggers": {
        "": {
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn": {
            "handlers": ["file_uvicorn_access"],
            "level": "ERROR",
        },
        "uvicorn.access": {
            "handlers": ["file_uvicorn_access"],
            "level": "INFO",
        },
        "uvicorn.error": {
            "handlers": ["file"],
            "level": "ERROR",
        },
        "aiosqlite": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
    },
}
