from typing import Any
from ferdelance.client.config import Config, ConfigError
from ferdelance.client.const import LOCAL_CONFIG_FILE

from argparse import ArgumentParser

import logging
import os
import re
import sys
import yaml

LOGGER = logging.getLogger(__name__)

VAR_PATTERN = re.compile(r".*?\${(\w+)}.*?")


class Arguments(ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(0, f"{self.prog}: error: {message}\n")


def check_for_environment_variables(value: str | bool | int | float) -> Any:
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


def setup_config_from_arguments() -> Config:
    """Defines the available input parameters base on command line arguments.

    Can raise ConfigError.
    """

    config: Config = Config()

    parser = Arguments()

    parser.add_argument(
        "-c",
        "--config",
        help=f"""
        Set a configuration file in YAML format to use
        Note that command line arguments take the precedence of arguments declared with a config file.
        """,
        default=None,
        type=str,
    )

    parser.add_argument(
        "--leave",
        help=f"""
        Request to disconnect this client from the server.
        This command will also remove the given working directory and all its content.
        (default: False)
        """,
        action="store_true",
        default=None,
    )

    # parse input arguments as a dict
    args = parser.parse_args()

    config_path: str = args.config

    if config_path is None:
        config_path = LOCAL_CONFIG_FILE

    if not os.path.exists(LOCAL_CONFIG_FILE):
        LOGGER.error(f"Configuration file not found at {config_path}")
        raise ConfigError()

    LOGGER.info(f"Using local config file found at {config_path}")

    # parse YAML config file
    with open(config_path, "r") as f:
        try:
            yaml_data: dict[str, Any] = yaml.safe_load(f)
            config_args: dict[str, Any] = yaml_data.get("ferdelance", dict())

            # assign values from config file
            client_args: dict[str, Any] = config_args.get("client", dict())

            config.server = check_for_environment_variables(client_args.get("server", config.server))

            config.heartbeat = check_for_environment_variables(client_args.get("heartbeat", config.heartbeat))

            config.workdir = check_for_environment_variables(client_args.get("workdir", config.workdir))
            config.private_key_location = check_for_environment_variables(client_args.get("private_key", None))

            # assign data sources
            datasources_args: list[dict[str, Any]] = config_args.get("datasource", list())
            for item in datasources_args:
                config.add_datasource(
                    datasource_id=item.get("id", ""),
                    name=item.get("name", ""),
                    type=item.get("type", ""),
                    kind=item.get("kind", ""),
                    conn=item.get("conn", ""),
                    path=item.get("path", ""),
                    token=item.get("token", ""),
                )

        except yaml.YAMLError as e:
            LOGGER.error(f"could not read config file {config}")
            LOGGER.exception(e)

    # assign values from command line
    if args.leave:
        config.leave = args.leave

    LOGGER.info("configuration completed")

    return config
