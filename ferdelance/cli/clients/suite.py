"""Clients CLI suite
"""

from ..base import CLIArgs, CLICommand, CLICommandSuite
from .functions import delete_client, describe_client, list_clients

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

rm_command: CLICommand = CLICommand(
    command="rm", arguments=[CLIArgs.CLIENT_ID], function=delete_client
)

#
#   SUITE
#


clients_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="clients",
    commands=[list_command, descr_command, rm_command],
)
