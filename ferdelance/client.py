from dotenv import load_dotenv

import argparse
import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)5s %(message)s',
)

LOGGER = logging.getLogger(__name__)


class Arguments(argparse.ArgumentParser):

    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(0, f"{self.prog}: error: {message}\n")


def setup_arguments():
    """Defines the available input parameters"""
    parser = Arguments(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-s', '--server', help='Set the url for the aggregation server', default="http://localhost/", type=str)
    return parser.parse_args()


class FerdelanceClient:

    def __init__(self, server: str) -> None:
        # possible states are: work, exit, update, install
        self.status: str = 'work'
        self.server: str = server

    def loop(self) -> None:
        while self.status != 'exit':
            if self.status == 'update':
                LOGGER.info('update application')
                sys.exit(1)

            if self.status == 'install':
                LOGGER.info('update and install packages')
                sys.exit(2)


if __name__ == '__main__':
    load_dotenv()
    args = setup_arguments()

    client = FerdelanceClient()
    client.loop()

    LOGGER.info('terminate application')
    sys.exit(0)
