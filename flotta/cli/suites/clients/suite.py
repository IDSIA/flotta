"""Clients CLI suite"""

from flotta.cli.base import CLICommand, CLICommandSuite
from flotta.cli.suites.args import CLIArgs
from flotta.cli.suites.commands import Commands

from .functions import describe_client, list_clients


#
#   COMMANDS
#

ls_command: CLICommand = CLICommand(
    command=Commands.list,
    arguments=[
        CLIArgs.CLIENT_ID,
    ],
    function=list_clients,
)

descr_command: CLICommand = CLICommand(
    command=Commands.describe,
    arguments=[
        CLIArgs.CLIENT_ID,
    ],
    function=describe_client,
)

#
#   SUITE
#


clients_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="clients",
    commands=[ls_command, descr_command],
)
