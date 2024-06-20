from flotta.cli.base import CLIArgument


class FDLCLIArgs:
    """All possible args in the flotta CLI"""

    ARTIFACT_ID: CLIArgument = CLIArgument(
        dash_string="--artifact-id",
        var_name="artifact_id",
        var_type=str,
        help="Artifact ID",
    )

    MODEL_ID: CLIArgument = CLIArgument(dash_string="--model-id", var_name="model_id", var_type=str, help="Model ID")

    CLIENT_ID: CLIArgument = CLIArgument(
        dash_string="--client-id", var_name="client_id", var_type=str, help="Client ID"
    )

    DATASOURCE_ID: CLIArgument = CLIArgument(
        dash_string="--datasource-id",
        var_name="datasource_id",
        var_type=str,
        help="Datasource ID",
    )

    AGGREGATE: CLIArgument = CLIArgument(
        dash_string="--aggregate",
        var_name="aggregate",
        var_type=bool,
        default=False,
        help="Create local model or aggregated model",
    )

    NAME: CLIArgument = CLIArgument(dash_string="--name", var_name="name", var_type=str, help="Component/Entity name")

    TOKEN: CLIArgument = CLIArgument(dash_string="--token", var_name="token", var_type=str, help="Project token")
    PROJECT_ID: CLIArgument = CLIArgument(
        dash_string="--project-id", var_name="project_id", var_type=str, help="Project ID"
    )
