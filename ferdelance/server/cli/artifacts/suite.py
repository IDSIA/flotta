"""Artifacts CLI suite
"""

from ..base import CLIArgs, CLICommand, CLICommandSuite
from .functions import get_artifact_description, get_artifacts_list

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="ls", arguments=[], function=get_artifacts_list
)

describe_command: CLICommand = CLICommand(
    command="descr", arguments=[CLIArgs.ARTIFACT_ID], function=get_artifact_description
)

remove_command: CLICommand = CLICommand(command="rm", arguments=[CLIArgs.ARTIFACT_ID])

#
#   SUITE
#


artifacts_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="artifacts",
    commands=[list_command, describe_command],
)
