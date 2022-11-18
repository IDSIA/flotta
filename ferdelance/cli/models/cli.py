"""
```
# LIST

python -m cli model list

python -m cli model list --id

python -m cli model list --artifact-id

# CREATE

python -m cli model create --artifact-id --client-id --aggregate

```
"""

from ..base import CLIArgument, CLICommand, CLICommandSuite
from .functions import models_list

#
#   ARGUMENTS
#

artifact_id: CLIArgument = CLIArgument(
    dash_string="--artifact-id",
    var_name="artifact_id",
    var_type=str,
    help="Artifact ID",
)

model_id: CLIArgument = CLIArgument(
    dash_string="--model-id", var_name="model_id", var_type=str, help="Model ID"
)

aggregate: CLIArgument = CLIArgument(
    dash_string="--aggregate",
    var_name="aggregate",
    var_type=bool,
    default=False,
    help="Create local model or aggregated model",
)

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="list", arguments=[artifact_id, model_id], function=models_list
)
create_command: CLICommand = CLICommand(
    command="create",
    arguments=[artifact_id, model_id, aggregate],
)

#
#   SUITE
#


models_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="models",
    commands=[list_command, create_command],
)
