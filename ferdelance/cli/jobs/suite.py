"""Jobs CLI suite
"""

from ..base import CLIArgs, CLICommand, CLICommandSuite
from .functions import get_jobs_list

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="ls",
    arguments=[CLIArgs.ARTIFACT_ID, CLIArgs.CLIENT_ID],
    function=get_jobs_list,
)

#
#   SUITE
#


jobs_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="jobs",
    commands=[list_command],
)
