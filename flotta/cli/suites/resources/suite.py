"""Models CLI suite"""

from flotta.cli.base import CLICommand, CLICommandSuite
from flotta.cli.suites.args import CLIArgs
from flotta.cli.suites.commands import Commands
from .functions import list_resource

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command=Commands.list,
    arguments=[CLIArgs.ARTIFACT_ID, CLIArgs.MODEL_ID],
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
