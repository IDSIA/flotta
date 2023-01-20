"""Datasources CLI suite
"""

from ferdelance.cli.base import CLIArgs, CLICommand, CLICommandSuite

from .functions import describe_datasource, list_datasources

#
#   COMMANDS
#

ls_command: CLICommand = CLICommand(
    command="ls",
    arguments=[CLIArgs.CLIENT_ID],
    function=list_datasources,
)

descr_command: CLICommand = CLICommand(
    command="descr",
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
