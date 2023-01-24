__all__ = [
    "clients_cli_suite",
    "artifacts_cli_suite",
    "jobs_cli_suite",
    "models_cli_suite",
    "projects_cli_suite",
    "datasources_cli_suite",
]


from .artifacts import artifacts_cli_suite
from .clients import clients_cli_suite
from .datasources import datasources_cli_suite
from .jobs import jobs_cli_suite
from .models import models_cli_suite
from .projects import projects_cli_suite
