"""
Create a command line script to administrate the server.
This is related to manage projects, users, clients, etc. As an example:
    - remove a client (deactivate, blacklisted)
    - remove a user
    - remove artifacts
    - remove models
    - stop jobs
    - clean space     
    - regenerate server ssh key
"""

from ferdelance.cli.base import CLIParser
from ferdelance.cli.models import models_cli_suite

parser: CLIParser = CLIParser(
    prog="Ferdelance Admin CLI",
    description="Command Line Interface to administrate the server",
)

parser.add_command_suite(models_cli_suite)

parser.parse_args()

print(parser.selected_command)
