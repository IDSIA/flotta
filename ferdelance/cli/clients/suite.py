"""Clients CLI suite
"""

from ..base import CLIArgs, CLICommand, CLICommandSuite
from .functions import describe_client, list_clients

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="ls",
    arguments=[
        CLIArgs.CLIENT_ID,
    ],
    function=list_clients,
)

descr_command: CLICommand = CLICommand(
    command="descr",
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
    commands=[list_command, descr_command],
)
