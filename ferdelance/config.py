from pydantic import BaseModel
from pytimeparse import parse

from dotenv import load_dotenv

import os

cpu_count = os.cpu_count()

load_dotenv()


class Configuration(BaseModel):
    STANDALONE: bool = "TRUE" == os.environ.get("STANDALONE", "False").upper()
    STANDALONE_WORKERS: int = int(os.environ.get("STANDALONE_WORKERS", 1 if cpu_count is None else cpu_count - 1))

    DISTRIBUTED: bool = "TRUE" == os.environ.get("DISTRIBUTED", "False").upper()

    SERVER_MAIN_PASSWORD: str | None = os.environ.get("SERVER_MAIN_PASSWORD", None)
    SERVER_PROTOCOL: str = os.environ.get("SERVER_PROTOCOL", "http")
    SERVER_INTERFACE: str = os.environ.get("SERVER_INTERFACE", "localhost")
    SERVER_PORT: int = int(os.environ.get("SERVER_PORT", 1456))

    WORKER_SERVER_PROTOCOL: str = os.environ.get("WORKER_SERVER_PROTOCOL", SERVER_PROTOCOL)
    WORKER_SERVER_HOST: str = os.environ.get("WORKER_SERVER_HOST", SERVER_INTERFACE)
    WORKER_SERVER_PORT: int = int(os.environ.get("WORKER_SERVER_PORT", SERVER_PORT))

    DB_USER: str | None = os.environ.get("DB_USER", None)
    DB_PASS: str | None = os.environ.get("DB_PASS", None)

    DB_DIALECT: str = os.environ.get("DB_DIALECT", "postgresql")
    DB_PORT: int = int(os.environ.get("DB_PORT", "5432"))
    DB_HOST: str | None = os.environ.get("DB_HOST", None)

    DB_SCHEMA: str = os.environ.get("DB_SCHEMA", "ferdelance")

    DB_MEMORY: bool = "TRUE" == os.environ.get("DB_MEMORY", "False").upper()

    STORAGE_BASE_DIR: str = os.environ.get("STORAGE_BASE_DIR", os.path.join(".", "storage"))
    STORAGE_DATASOURCES: str = str(os.path.join(STORAGE_BASE_DIR, "datasources"))
    STORAGE_ARTIFACTS: str = str(os.path.join(STORAGE_BASE_DIR, "artifacts"))
    STORAGE_CLIENTS: str = str(os.path.join(STORAGE_BASE_DIR, "clients"))
    STORAGE_RESULTS: str = str(os.path.join(STORAGE_BASE_DIR, "results"))

    FILE_CHUNK_SIZE: int = int(os.environ.get("FILE_CHUNK_SIZE", 4096))

    CLIENT_TOKEN_EXPIRATION = os.environ.get("TOKEN_CLIENT_EXPIRATION", str(parse("90 day")))
    USER_TOKEN_EXPIRATION = os.environ.get("TOKEN_USER_EXPIRATION", str(parse("30 day")))

    PROJECT_DEFAULT_TOKEN: str = os.environ.get("PROJECT_DEFAULT_TOKEN", "")

    def storage_dir_datasources(self, datasource_hash: str) -> str:
        return os.path.join(conf.STORAGE_DATASOURCES, datasource_hash)

    def storage_dir_artifact(self, artifact_id: str) -> str:
        return os.path.join(conf.STORAGE_ARTIFACTS, artifact_id)

    def storage_dir_clients(self, client_id: str) -> str:
        return os.path.join(conf.STORAGE_CLIENTS, client_id)

    def storage_dir_results(self, result_id: str) -> str:
        return os.path.join(conf.STORAGE_RESULTS, result_id)

    def server_url(self) -> str:
        return f"{self.WORKER_SERVER_PROTOCOL}://{self.WORKER_SERVER_HOST.rstrip('/')}:{self.WORKER_SERVER_PORT}"


conf: Configuration = Configuration()

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
