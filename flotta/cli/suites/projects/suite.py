"""Projects CLI suite"""

from flotta.cli.base import CLICommand, CLICommandSuite
from flotta.cli.suites.args import FDLCLIArgs
from flotta.cli.suites.commands import FDLCommands
from .functions import create_project, list_projects, describe_project

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command=FDLCommands.list,
    arguments=[],
    function=list_projects,
)

create_command: CLICommand = CLICommand(
    command=FDLCommands.create,
    arguments=[FDLCLIArgs.NAME],
    function=create_project,
)

describe_command: CLICommand = CLICommand(
    command=FDLCommands.describe,
    arguments=[FDLCLIArgs.PROJECT_ID, FDLCLIArgs.TOKEN],
    function=describe_project,
)

#
#   SUITE
#


projects_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="projects",
    commands=[list_command, create_command, describe_command],
)
