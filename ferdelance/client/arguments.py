from typing import Any
from ferdelance.client.config import Config, ConfigError
from ferdelance.client.exceptions import ErrorClient
from ferdelance.schemas.client import ArgumentsConfig

from argparse import ArgumentParser

import logging
import os
import sys
import yaml

LOCAL_CONFIG_FILE: str = os.path.join(".", "config.yaml")

LOGGER = logging.getLogger(__name__)


class CustomArguments(ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(0, f"{self.prog}: error: {message}\n")


def setup_config_from_arguments() -> Config:
    """Defines the available input parameters base on command line arguments.

    Can raise ConfigError.
    """

    parser = CustomArguments()

    parser.add_argument(
        "-c",
        "--config",
        help=f"""
        Set a configuration file in YAML format to use
        Note that command line arguments take the precedence of arguments declared with a config file.
        """,
        default=LOCAL_CONFIG_FILE,
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

    if not os.path.exists(config_path):
        LOGGER.error(f"Configuration file not found at {config_path}")
        raise ConfigError()

    LOGGER.info(f"Using local config file found at {config_path}")

    # parse YAML config file
    with open(config_path, "r") as f:
        try:
            yaml_data: dict[str, Any] = yaml.safe_load(f)

            args_config: ArgumentsConfig = ArgumentsConfig(**yaml_data)

            config: Config = Config(args_config)

            # assign values from command line
            if args.leave:
                config.leave = args.leave

            LOGGER.info("configuration completed")

            return config

        except yaml.YAMLError as e:
            LOGGER.error(f"could not read config file {config_path}")
            LOGGER.exception(e)
            raise ErrorClient()
