from dataclasses import dataclass
from typing import Callable, List

from ferdelance.cli.base import CLIArgument


async def function_not_implemented():
    raise NotImplementedError("This command has not been implemented.")


@dataclass
class CLICommand:
    """Allow CLI submodules to determine which commands go with which arguments"""

    command: str
    arguments: List[CLIArgument]
    function: Callable = function_not_implemented
