"""Jobs CLI suite
"""

from ..base import CLIArgs, CLICommand, CLICommandSuite
from .functions import list_jobs

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command="ls",
    arguments=[CLIArgs.ARTIFACT_ID, CLIArgs.CLIENT_ID],
    function=list_jobs,
)

#
#   SUITE
#


jobs_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="jobs",
    commands=[list_command],
)
