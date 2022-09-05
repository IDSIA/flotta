from argparse import ArgumentParser
from typing import Any

import logging
import sys
import yaml

LOGGER = logging.getLogger(__name__)


class Arguments(ArgumentParser):

    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(0, f"{self.prog}: error: {message}\n")


def setup_arguments() -> dict[str, Any]:
    """Defines the available input parameters base on command line arguments."""

    arguments: dict[str, Any] = {
        'server': 'http://localhost/',
        'workdir': './FDL.client.workdir',
        'config': None,
        'heartbeat': 1.0,
        'leave': False,

        'datasources': [],
    }
    LOGGER.debug(f'default arguments: {arguments}')

    parser = Arguments()

    parser.add_argument(
        '-s', '--server',
        help=f"""
        Set the url for the aggregation server.
        (default: {arguments['server']})
        """,
        default=None,
        type=str,
    )
    parser.add_argument(
        '-w', '--workdir',
        help=f"""
        Set the working directory of the client
        (default: {arguments['workdir']})
        """,
        default=None,
        type=str,
    )
    parser.add_argument(
        '-c', '--config',
        help=f"""
        Set a configuration file in YAML format to use
        Note that command line arguments take the precedence of arguments declared with a config file.
        (default: {arguments['config']})
        """,
        default=None,
        type=str,
    )
    parser.add_argument(
        '-b', '--heartbeat',
        help=f"""
        Set the amount of time in seconds to wait after a command execution.
        (default: {arguments['heartbeat']})
        """,
        default=None,
        type=float
    )

    parser.add_argument(
        '--leave',
        help=f"""
        Request to disconnect this client from the server.
        This command will also remove the given working directory and all its content.
        (default: {arguments['leave']})
        """,
        action='store_true',
        default=None,
    )
    parser.add_argument(
        '-f', '--file',
        help="""
        Add a file as source file.
        This arguments takes 3 positions: a unique NAME, a file type (TYPE), and a file path (PATH).
        The supported filetypes are: CSV, TSV.
        This arguments can be repeated.
        """,
        default=None,
        action='append',
        nargs=3,
        metavar=('NAME', 'TYPE', 'PATH'),
        type=str,
    )
    parser.add_argument(
        '-db', '--dbms',
        help="""
        Add a database as source file.
        This arguments takes 3 position: a unique NAME, a supported DBMS (TYPE), and the connection string (CONN).
        The DBMS requires full compatiblity with SQLAlchemy
        This arguments can be repeated.
        """,
        default=None,
        action='append',
        nargs=3,
        metavar=('NAME', 'TYPE', 'CONN'),
        type=str
    )

    # parse input arguments as a dict
    args = parser.parse_args()

    server = args.server
    workdir = args.workdir
    config = args.config
    heartbeat = args.heartbeat
    leave = args.leave
    files = args.file
    dbs = args.dbms

    # parse YAML config file
    if config is not None:
        with open(config, 'r') as f:
            try:
                config_args = yaml.safe_load(f)['ferdelance']
                LOGGER.debug(f'config arguments: {config_args}')
            except yaml.YAMLError as e:
                LOGGER.error(f'could not read config file {config}')
                LOGGER.exception(e)

        # assign values from config file
        arguments['config'] = config
        arguments['server'] = config_args['client']['server']
        arguments['workdir'] = config_args['client']['workdir']
        arguments['heartbeat'] = config_args['client']['heartbeat']

        # assign data sources
        for item in config_args['datasource']['file']:
            arguments['datasources'].append(('file', item['name'], item['type'], item['path']))
        for item in config_args['datasource']['db']:
            arguments['datasources'].append(('db', item['name'], item['type'], item['conn']))

    LOGGER.debug(f'config arguments: {arguments}')

    # assign values from command line
    if server:
        arguments['server'] = server
    if workdir:
        arguments['workdir'] = workdir
    if heartbeat:
        arguments['heartbeat'] = heartbeat
    if leave:
        arguments['leave'] = leave

    if files:
        for name, type, path in files:
            arguments['datasources'].append(('file', name, type, path))
    if dbs:
        for t, conn in files:
            arguments['datasources'].append(('db', name, type, conn))

    LOGGER.info(f'arguments: {arguments}')

    return arguments
