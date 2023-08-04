from .config import config_manager, Configuration

from argparse import ArgumentParser

import logging
import os
import sys

LOCAL_CONFIG_FILE: str = os.path.join(".", "config.yaml")

LOGGER = logging.getLogger(__name__)


class CustomArguments(ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(0, f"{self.prog}: error: {message}\n")


def setup_config_from_arguments() -> tuple[Configuration, bool]:
    """Defines the available input parameters base on command line arguments.

    Can raise ConfigError.
    """

    parser = CustomArguments()

    parser.add_argument(
        "-c",
        "--config",
        help="""
        Set a configuration file in YAML format to use
        Note that command line arguments take the precedence of arguments declared with a config file.
        """,
        default=LOCAL_CONFIG_FILE,
        type=str,
    )

    parser.add_argument(
        "--leave",
        help="""
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

    config: Configuration = config_manager.reload(config_path)

    return config, args.leave
