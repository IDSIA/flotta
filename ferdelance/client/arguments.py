from argparse import ArgumentParser
from typing import Any

import logging
import os
import re
import sys
import yaml

LOGGER = logging.getLogger(__name__)

VAR_PATTERN = re.compile('.*?\${(\w+)}.*?')

LOCAL_CONFIG_FILE = os.path.join('.', 'config.yaml')

FDL_SERVER = 'FDL_SERVER'
FDL_HEARTBEAT = 'FDL_HEARTBEAT'


class Arguments(ArgumentParser):

    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(0, f"{self.prog}: error: {message}\n")


def check_for_environment_variables(value: str | bool | int | float):
    """Source: https://dev.to/mkaranasou/python-yaml-configuration-with-environment-variables-parsing-2ha6"""
    if not isinstance(value, str):
        return value

    # find all env variables in line
    match = VAR_PATTERN.findall(value)

    # TODO: testing required

    if match:
        full_value = value
        for g in match:
            full_value = full_value.replace(
                f'${{{g}}}', os.environ.get(g, g)
            )
        return full_value
    return value


def setup_arguments() -> dict[str, Any]:
    """Defines the available input parameters base on command line arguments."""

    arguments: dict[str, Any] = {
        'server': 'http://localhost/',
        'workdir': 'workdir',
        'heartbeat': 1.0,
        'leave': False,

        'datasources': [],
    }
    LOGGER.debug(f'default arguments: {arguments}')

    parser = Arguments()

    parser.add_argument(
        '-c', '--config',
        help=f"""
        Set a configuration file in YAML format to use
        Note that command line arguments take the precedence of arguments declared with a config file.
        """,
        default=None,
        type=str,
    )

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

    config = args.config

    if config is None and os.path.exists(LOCAL_CONFIG_FILE):
        config = LOCAL_CONFIG_FILE

    # parse YAML config file
    if config is not None:
        with open(config, 'r') as f:
            try:
                config_args = yaml.safe_load(f)['ferdelance']
                LOGGER.debug(f'config input arguments: {config_args}')
            except yaml.YAMLError as e:
                LOGGER.error(f'could not read config file {config}')
                LOGGER.exception(e)

        # assign values from config file
        arguments['server'] = check_for_environment_variables(config_args['client']['server'])
        arguments['workdir'] = check_for_environment_variables(config_args['client']['workdir'])
        arguments['heartbeat'] = check_for_environment_variables(config_args['client']['heartbeat'])

        # assign data sources
        for item in config_args['datasource']:
            arguments['datasources'].append((
                check_for_environment_variables(item['kind']),
                check_for_environment_variables(item['name']),
                check_for_environment_variables(item['type']),
                check_for_environment_variables(item['conn'] if item['kind'] == 'db' else item['path']),
            ))

    LOGGER.debug(f'config output arguments: {arguments}')

    # assign values from command line
    if args.server:
        arguments['server'] = check_for_environment_variables(args.server)
    if args.workdir:
        arguments['workdir'] = check_for_environment_variables(args.workdir)
    if args.heartbeat:
        arguments['heartbeat'] = check_for_environment_variables(args.heartbeat)
    if args.leave:
        arguments['leave'] = args.leave

    if args.file:
        for name, type, path in args.file:
            arguments['datasources'].append({
                'kind': 'file',
                'name': check_for_environment_variables(name),
                'type': check_for_environment_variables(type),
                'path': check_for_environment_variables(path),
            })
    if args.dbms:
        for name, type, conn in args.dbms:
            arguments['datasources'].append({
                'kind': 'db',
                'name': check_for_environment_variables(name),
                'type': check_for_environment_variables(type),
                'path': check_for_environment_variables(conn),
            })

    arguments['server'] = os.environ.get(FDL_SERVER, arguments['server'])
    arguments['heartbeat'] = float(os.environ.get(FDL_HEARTBEAT, arguments['heartbeat']))

    LOGGER.debug(f'used arguments: {arguments}')

    return arguments
