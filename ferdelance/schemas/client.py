from ferdelance.schemas.artifacts import Artifact

from pydantic import BaseModel, validator

import os
import re

VAR_PATTERN = re.compile(r".*?\${(\w+)}.*?")


class ClientJoinRequest(BaseModel):
    """Data sent by the client to join the server."""

    system: str
    mac_address: str
    node: str

    public_key: str  # b64encoded bytes
    version: str


class ClientJoinData(BaseModel):
    """Data sent by the server to the client after a successful join."""

    id: str
    token: str
    public_key: str


class ClientDetails(BaseModel):
    client_id: str
    version: str


class ClientUpdate(BaseModel):
    action: str


class ClientUpdateTaskCompleted(ClientUpdate):
    client_task_id: str
    # TODO: consider return errors to workbench


class ClientTask(BaseModel):
    artifact: Artifact
    datasource_hashes: list[str]


class DataSourceConfig(BaseModel):
    name: str
    token: list[str] | str | None
    kind: str
    type: str
    conn: str | None = None
    path: str | None = None


class ArgumentsConfig(BaseModel):
    server: str = "http://localhost/"
    heartbeat: float = 1.0
    workdir: str = "./workdir"
    private_key_location: str | None

    datasources: list[DataSourceConfig]

    @validator("server", "heartbeat", "workdir", "private_key_location", pre=True)
    @classmethod
    def check_for_env_variables(cls, value):
        """Source: https://dev.to/mkaranasou/python-yaml-configuration-with-environment-variables-parsing-2ha6"""
        if not isinstance(value, str):
            return value

        # find all env variables in line
        match = VAR_PATTERN.findall(value)

        # TODO: testing required

        if match:
            full_value = value
            for g in match:
                full_value = full_value.replace(f"${{{g}}}", os.environ.get(g, g))
            return full_value
        return value
