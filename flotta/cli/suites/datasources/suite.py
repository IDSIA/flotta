"""Datasources CLI suite"""

from flotta.cli.base import CLICommand, CLICommandSuite
from flotta.cli.suites.args import CLIArgs
from flotta.cli.suites.commands import Commands
from .functions import describe_datasource, list_datasources

#
#   COMMANDS
#

ls_command: CLICommand = CLICommand(
    command=Commands.list,
    arguments=[CLIArgs.CLIENT_ID],
    function=list_datasources,
)

descr_command: CLICommand = CLICommand(
    command=Commands.describe,
    arguments=[CLIArgs.DATASOURCE_ID],
    function=describe_datasource,
)

#
#   SUITE
#


datasources_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="datasources",
    commands=[ls_command, descr_command],
)
