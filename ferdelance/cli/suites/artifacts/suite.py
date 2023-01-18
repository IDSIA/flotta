"""Artifacts CLI suite
"""

from ...base import CLIArgs, CLICommand, CLICommandSuite
from .functions import describe_artifact, list_artifacts

#
#   COMMANDS
#

ls_command: CLICommand = CLICommand(command="ls", arguments=[], function=list_artifacts)

descr_command: CLICommand = CLICommand(command="descr", arguments=[CLIArgs.ARTIFACT_ID], function=describe_artifact)

#
#   SUITE
#


artifacts_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="artifacts",
    commands=[ls_command, descr_command],
)
