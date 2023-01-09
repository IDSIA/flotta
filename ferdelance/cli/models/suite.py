"""Models CLI suite

Available commands:
    python -m cli model list
    python -m cli model list --id
    python -m cli model list --artifact-id
    python -m cli model describe --artifact-id
"""

from ..base.args import artifact_id, model_id
from ..base.models import CLIArgument, CLICommand, CLICommandSuite
from .functions import models_list

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="list", arguments=[artifact_id, model_id], function=models_list
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
