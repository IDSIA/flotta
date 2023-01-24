"""Projects CLI suite
"""


from ...base import CLIArgs, CLICommand, CLICommandSuite
from .functions import create_project, list_projects

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="ls",
    arguments=[],
    function=list_projects,
)

create_command: CLICommand = CLICommand(
    command="new",
    arguments=[CLIArgs.NAME],
    function=create_project,
)

#
#   SUITE
#


projects_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="projects",
    commands=[list_command, create_command],
)
