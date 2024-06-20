"""Datasources CLI suite"""

from flotta.cli.base import CLICommand, CLICommandSuite
from flotta.cli.fdl_suites.args import FDLCLIArgs
from flotta.cli.fdl_suites.commands import FDLCommands
from .functions import describe_datasource, list_datasources

#
#   COMMANDS
#

ls_command: CLICommand = CLICommand(
    command=FDLCommands.list,
    arguments=[FDLCLIArgs.CLIENT_ID],
    function=list_datasources,
)

descr_command: CLICommand = CLICommand(
    command=FDLCommands.describe,
    arguments=[FDLCLIArgs.DATASOURCE_ID],
    function=describe_datasource,
)

#
#   SUITE
#


datasources_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="datasources",
    commands=[ls_command, descr_command],
)
