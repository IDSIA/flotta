""" Base code to be shared across CLI submodules
"""

from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Type


async def function_not_implemented():
    raise NotImplementedError("This command has not been implemented.")


@dataclass
class CLIArgument:
    """Argument that will be linked to a specific command"""

    dash_string: str
    var_name: str
    var_type: Type
    default: Any = None
    help: str = None

    def __eq__(self, other: object) -> bool:
        return self.dash_string == other.dash_string

    def __hash__(self):
        return hash(self.dash_string)


@dataclass
class CLICommand:
    """Allow CLI submodules to determine which commands go with which arguments"""

    command: str
    arguments: List[CLIArgument]
    function: Callable = function_not_implemented


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
        self.parser.add_argument("command", metavar="C", type=str, help="Which command to execute")

        self.suites: List[CLICommandSuite] = []
        self.selected_suite: CLICommandSuite = None
        self.selected_command: CLICommand = None
        self.args = Dict[str, Any]

        self.avail_args_set: set = set()

    def add_command_suite(self, command_suite: CLICommandSuite) -> None:
        """Prepare the parser to receive all arguments in suite commands

        Args:
            command_suite (CLICommandSuite): The suite object
        """
        for command in command_suite.commands:
            for arg in command.arguments:
                if arg.dash_string not in self.avail_args_set:
                    self.avail_args_set.add(arg.dash_string)
                    self.parser.add_argument(
                        arg.dash_string,
                        dest=arg.var_name,
                        type=arg.var_type,
                        default=arg.default,
                        help=arg.help,
                    )
        self.suites.append(command_suite)

    def parse_args(self, command_line_args: List[str] = None) -> None:
        """Parse command line arguments with argparse and identify which action has to be performed with which arguments

        Raises:
            ValueError: If the entity is not among the ones supported by added suites
            ValueError: If the command is not among supported commands of the entity selected suite
        """
        args: Namespace = self.parser.parse_args(command_line_args)

        selected_suite: CLICommandSuite = next(filter(lambda s: s.entity == args.entity, self.suites), None)
        if selected_suite is None:
            raise ValueError(f"No command suite found for entity '{args.entity}'")

        selected_command: CLICommand = next(filter(lambda c: c.command == args.command, selected_suite.commands), None)

        if selected_command is None:
            raise ValueError(f"Command suite {selected_suite.entity} does not support command '{args.command}'")

        self.selected_suite = selected_suite
        self.selected_command = selected_command
        self.args = {sca.var_name: getattr(args, sca.var_name) for sca in self.selected_command.arguments}


@dataclass
class CLIArgs:
    ARTIFACT_ID: CLIArgument = CLIArgument(
        dash_string="--artifact-id",
        var_name="artifact_id",
        var_type=str,
        help="Artifact ID",
    )

    MODEL_ID: CLIArgument = CLIArgument(dash_string="--model-id", var_name="model_id", var_type=str, help="Model ID")

    CLIENT_ID: CLIArgument = CLIArgument(
        dash_string="--client-id", var_name="client_id", var_type=str, help="Client ID"
    )

    AGGREGATE: CLIArgument = CLIArgument(
        dash_string="--aggregate",
        var_name="aggregate",
        var_type=bool,
        default=False,
        help="Create local model or aggregated model",
    )

    NAME: CLIArgument = CLIArgument(
        dash_string="--name",
        var_name="name",
        var_type=str,
        help="Name of the entity",
    )
