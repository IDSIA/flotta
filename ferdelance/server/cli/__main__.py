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

from dotenv import load_dotenv

from .artifacts import artifacts_cli_suite
from .base import CLIParser
from .clients import clients_cli_suite
from .jobs import jobs_cli_suite
from .models import models_cli_suite

load_dotenv()


async def main() -> None:
    """Execute CLI command"""
    parser: CLIParser = CLIParser(
        prog="Ferdelance Admin CLI",
        description="Command Line Interface to administrate the server",
    )

    parser.add_command_suite(models_cli_suite)
    parser.add_command_suite(artifacts_cli_suite)
    parser.add_command_suite(jobs_cli_suite)
    parser.add_command_suite(clients_cli_suite)

    parser.parse_args()

    await parser.selected_command.function(**parser.args)


if __name__ == "__main__":
    loop = asyncio.run(main())
