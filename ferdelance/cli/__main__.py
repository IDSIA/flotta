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
from typing import List

from .artifacts import artifacts_cli_suite
from .base import CLICommandSuite, CLIParser
from .clients import clients_cli_suite
from .jobs import jobs_cli_suite
from .models import models_cli_suite


async def main() -> None:
    """Execute CLI command"""
    parser: CLIParser = CLIParser(
        prog="Ferdelance CLI",
        description="Command Line Interface to administrate client/server",
    )

    suites: List[CLICommandSuite] = [
        models_cli_suite,
        artifacts_cli_suite,
        jobs_cli_suite,
        clients_cli_suite,
    ]

    for suite in suites:
        parser.add_command_suite(suite)

    parser.parse_args()

    await parser.selected_command.function(**parser.args)


if __name__ == "__main__":
    loop = asyncio.run(main())
