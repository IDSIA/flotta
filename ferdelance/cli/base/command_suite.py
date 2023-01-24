from dataclasses import dataclass
from typing import List

from ferdelance.cli.base import CLICommand


@dataclass
class CLICommandSuite:
    """Suite of all supported commands"""

    entity: str
    commands: List[CLICommand]
