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

from ferdelance.cli.base import CLIArgument, CLICommand, CLICommandSuite

artifact_id: CLIArgument = CLIArgument(
    dash_string="--artifact-id",
    var_name="artifact_id",
    var_type=str,
    help="Artifact ID",
)

client_id: CLIArgument = CLIArgument(
    dash_string="--client-id", var_name="client_id", var_type=str, help="Client ID"
)

aggregate: CLIArgument = CLIArgument(
    dash_string="--aggregate",
    var_name="aggregate",
    var_type=bool,
    default=False,
    help="Create local model or aggregated model",
)

list_command: CLICommand = CLICommand(
    command="list", arguments=[artifact_id, client_id]
)
create_command: CLICommand = CLICommand(
    command="create",
    arguments=[artifact_id, client_id, aggregate],
)


models_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="models",
    commands=[list_command, create_command],
)
