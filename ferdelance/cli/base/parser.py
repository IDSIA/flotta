from argparse import ArgumentParser, Namespace
from typing import Any
from ferdelance.cli.base import CLICommand, CLICommandSuite


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

        self.suites: list[CLICommandSuite] = []
        self.selected_suite: CLICommandSuite = None
        self.selected_command: CLICommand = None
        self.args = dict[str, Any]

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

    def parse_args(self, command_line_args: list[str] = None) -> None:
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
