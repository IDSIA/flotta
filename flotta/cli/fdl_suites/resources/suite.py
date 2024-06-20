"""Models CLI suite"""

from ferdelance.cli.base import CLICommand, CLICommandSuite
from ferdelance.cli.fdl_suites.args import FDLCLIArgs
from ferdelance.cli.fdl_suites.commands import FDLCommands
from .functions import list_resource

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command=FDLCommands.list,
    arguments=[FDLCLIArgs.ARTIFACT_ID, FDLCLIArgs.MODEL_ID],
    function=list_resource,
)

#
#   SUITE
#


models_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="models",
    commands=[
        list_command,
    ],
)
