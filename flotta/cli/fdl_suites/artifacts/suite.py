"""Artifacts CLI suite"""

from ferdelance.cli.base import CLICommand, CLICommandSuite
from ferdelance.cli.fdl_suites.args import FDLCLIArgs
from ferdelance.cli.fdl_suites.commands import FDLCommands
from .functions import describe_artifact, list_artifacts

#
#   COMMANDS
#

ls_command: CLICommand = CLICommand(command=FDLCommands.list, arguments=[], function=list_artifacts)

descr_command: CLICommand = CLICommand(
    command=FDLCommands.describe, arguments=[FDLCLIArgs.ARTIFACT_ID], function=describe_artifact
)

#
#   SUITE
#


artifacts_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="artifacts",
    commands=[ls_command, descr_command],
)
