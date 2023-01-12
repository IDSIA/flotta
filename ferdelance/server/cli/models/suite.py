"""Models CLI suite
"""

from ..base import CLIArgs, CLICommand, CLICommandSuite
from .functions import models_list

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="ls",
    arguments=[CLIArgs.ARTIFACT_ID, CLIArgs.MODEL_ID],
    function=models_list,
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
