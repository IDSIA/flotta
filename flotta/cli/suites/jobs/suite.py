"""Jobs CLI suite"""

from flotta.cli.base import CLICommand, CLICommandSuite
from flotta.cli.suites.args import CLIArgs
from flotta.cli.suites.commands import Commands

from .functions import list_jobs

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command=Commands.list,
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
