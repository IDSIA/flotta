"""Models CLI suite
"""

from ...base import CLIArgs, CLICommand, CLICommandSuite
from .functions import list_models

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="ls",
    arguments=[CLIArgs.ARTIFACT_ID, CLIArgs.MODEL_ID],
    function=list_models,
)

#
#   SUITE
#


models_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="models",
    commands=[
        list_command,
    ],
)
