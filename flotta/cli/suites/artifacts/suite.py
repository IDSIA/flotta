"""Artifacts CLI suite"""

from flotta.cli.base import CLICommand, CLICommandSuite
from flotta.cli.suites.args import CLIArgs
from flotta.cli.suites.commands import Commands
from .functions import describe_artifact, list_artifacts

#
#   COMMANDS
#

ls_command: CLICommand = CLICommand(command=Commands.list, arguments=[], function=list_artifacts)

descr_command: CLICommand = CLICommand(
    command=Commands.describe, arguments=[CLIArgs.ARTIFACT_ID], function=describe_artifact
)

#
#   SUITE
#


artifacts_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="artifacts",
    commands=[ls_command, descr_command],
)
