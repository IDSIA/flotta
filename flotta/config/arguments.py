from argparse import ArgumentParser
from pathlib import Path

import sys


class CustomArguments(ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(0, f"{self.prog}: error: {message}\n")


def setup_config_from_arguments() -> tuple[Path | None, bool]:
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
        default="",
        type=str,
    )

    # TODO: add working directory as parameter

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
    args, _ = parser.parse_known_args()

    config_path = None if not args.config else Path(args.config)

    return config_path, args.leave or False
