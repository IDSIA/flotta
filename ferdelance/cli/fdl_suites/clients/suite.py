"""Clients CLI suite"""

from ferdelance.cli.base import CLICommand, CLICommandSuite

from ferdelance.cli.fdl_suites.args import FDLCLIArgs
from ferdelance.cli.fdl_suites.commands import FDLCommands

from .functions import describe_client, list_clients


#
#   COMMANDS
#

ls_command: CLICommand = CLICommand(
    command=FDLCommands.list,
    arguments=[
        FDLCLIArgs.CLIENT_ID,
    ],
    function=list_clients,
)

descr_command: CLICommand = CLICommand(
    command=FDLCommands.describe,
    arguments=[
        FDLCLIArgs.CLIENT_ID,
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
