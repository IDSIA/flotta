"""Artifacts CLI suite

Available commands:
    python -m cli artifact list
    python -m cli artifact describe --artifact-id
    python -m cli artifact delete --artifact-id
"""

from ..base.args import artifact_id
from ..base.models import CLIArgument, CLICommand, CLICommandSuite
from .functions import get_artifact_description, get_artifacts_list

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="list", arguments=[], function=get_artifacts_list
)

describe_command: CLICommand = CLICommand(
    command="describe", arguments=[artifact_id], function=get_artifact_description
)

#
#   SUITE
#


artifacts_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="artifacts",
    commands=[list_command, describe_command],
)
