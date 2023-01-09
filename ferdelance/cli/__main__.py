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
import asyncio

from .artifacts import artifacts_cli_suite
from .base.models import CLIParser
from .models import models_cli_suite


async def main() -> None:

    parser: CLIParser = CLIParser(
        prog="Ferdelance Admin CLI",
        description="Command Line Interface to administrate the server",
    )

    parser.add_command_suite(models_cli_suite)
    parser.add_command_suite(artifacts_cli_suite)

    parser.parse_args()

    await parser.selected_command.function(**parser.args)


if __name__ == "__main__":
    loop = asyncio.run(main())
