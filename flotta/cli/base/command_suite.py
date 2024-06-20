from dataclasses import dataclass

from flotta.cli.base import CLICommand


@dataclass
class CLICommandSuite:
    """Suite of all supported commands"""

    entity: str
    commands: list[CLICommand]
