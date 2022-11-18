""" Base code to be shared across CLI submodules
"""

from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Type


def command_not_implemented():
    raise NotImplementedError()


@dataclass
class CLIArgument:
    """Argument that will be linked to a specific command"""

    dash_string: str
    var_name: str
    var_type: Type
    default: Any = None
    help: str = None

    def __eq__(self, __o: object) -> bool:
        return self.dash_string == __o.dash_string

    def __hash__(self):
        return hash(self.dash_string)


@dataclass
class CLICommand:
    """Allow CLI submodules to determine which commands go with which arguments"""

    command: str
    arguments: List[CLIArgument]
    function: Callable = command_not_implemented


@dataclass
class CLICommandSuite:
    """Suite of all supported commands"""

    entity: str
    commands: List[CLICommand]


class CLIParser:
    def __init__(
        self,
        prog: str,
        description: str,
    ) -> None:
        self.parser: ArgumentParser = ArgumentParser(prog, description)
        self.parser.add_argument(
            "entity",
            metavar="E",
            type=str,
            help="Which entity is associated to the command",
        )
        self.parser.add_argument(
            "command", metavar="C", type=str, help="Which command to execute"
        )

        self.suites: List[CLICommandSuite] = []
        self.selected_suite: CLICommandSuite = None
        self.selected_command: CLICommand = None
        self.args = Dict[str, Any]

    def add_command_suite(self, command_suite: CLICommandSuite) -> None:

        args_set: set = set()

        for command in command_suite.commands:
            for arg in command.arguments:
                args_set.add(arg)

        for arg in args_set:
            self.parser.add_argument(
                arg.dash_string,
                dest=arg.var_name,
                type=arg.var_type,
                default=arg.default,
                help=arg.help,
            )

        self.suites.append(command_suite)

    def parse_args(
        self,
    ) -> None:

        args: Namespace = self.parser.parse_args()

        selected_suite: CLICommandSuite = next(
            filter(lambda s: s.entity == args.entity, self.suites), None
        )
        if selected_suite is None:
            raise ValueError(f"No command suite found for entity '{args.entity}'")

        selected_command: CLICommand = next(
            filter(lambda c: c.command == args.command, selected_suite.commands), None
        )

        if selected_command is None:
            raise ValueError(
                f"Command suite {selected_suite.entity} does not support command '{args.command}'"
            )

        self.selected_suite = selected_suite
        self.selected_command = selected_command
        self.args = {
            sca.var_name: getattr(args, sca.var_name)
            for sca in self.selected_command.arguments
        }
