"""Projects CLI suite"""

from flotta.cli.base import CLICommand, CLICommandSuite
from flotta.cli.suites.args import CLIArgs
from flotta.cli.suites.commands import Commands
from .functions import create_project, list_projects, describe_project

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command=Commands.list,
    arguments=[],
    function=list_projects,
)

create_command: CLICommand = CLICommand(
    command=Commands.create,
    arguments=[CLIArgs.NAME],
    function=create_project,
)

describe_command: CLICommand = CLICommand(
    command=Commands.describe,
    arguments=[CLIArgs.PROJECT_ID, CLIArgs.TOKEN],
    function=describe_project,
)

#
#   SUITE
#


projects_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="projects",
    commands=[list_command, create_command, describe_command],
)
