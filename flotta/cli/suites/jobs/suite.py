"""Jobs CLI suite"""

from flotta.cli.base import CLICommand, CLICommandSuite
from flotta.cli.suites.args import FDLCLIArgs
from flotta.cli.suites.commands import FDLCommands

from .functions import list_jobs

#
#   COMMANDS
#

list_command: CLICommand = CLICommand(
    command=FDLCommands.list,
    arguments=[FDLCLIArgs.ARTIFACT_ID, FDLCLIArgs.CLIENT_ID],
    function=list_jobs,
)

#
#   SUITE
#


jobs_cli_suite: CLICommandSuite = CLICommandSuite(
    entity="jobs",
    commands=[list_command],
)
