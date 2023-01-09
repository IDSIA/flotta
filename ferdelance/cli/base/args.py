from .models import CLIArgument

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
