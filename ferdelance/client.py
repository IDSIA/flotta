from typing import Any
from ferdelance import __version__
from ferdelance_shared.generate import (
    generate_asymmetric_key,
    bytes_from_private_key,
    bytes_from_public_key,
    private_key_from_bytes,
    public_key_from_str,
    RSAPrivateKey,
    RSAPublicKey,
)
from ferdelance_shared.decode import (
    decrypt,
    decode_from_transfer,
    decrypt_stream,
)
from ferdelance_shared.encode import (
    encrypt,
    encode_to_transfer,
    stream_encrypt_file,
)

from base64 import b64encode
from getmac import get_mac_address
from dotenv import load_dotenv
from requests import Response
from time import sleep

import yaml

import argparse
import json
import logging
import requests
import os
import platform
import shutil
import sys
import uuid

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s %(name)8s %(levelname)6s %(message)s',
)

LOGGER = logging.getLogger(__name__)


class Arguments(argparse.ArgumentParser):

    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(0, f"{self.prog}: error: {message}\n")


def setup_arguments() -> dict[str, Any]:
    arguments: dict[str, Any] = {
        'server': 'http://localhost/',
        'workdir': './FDL.client.workdir',
        'config': None,
        'heartbeat': 1.0,
        'leave': False,

        'datasource': [],
    }
    LOGGER.debug(f'default arguments: {arguments}')

    """Defines the available input parameters"""
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
        This arguments takes 2 positions: a file type (TYPE) and a file path (PATH).
        The supported filetypes are: CSV, TSV.
        This arguments can be repeated.
        """,
        default=None,
        action='append',
        nargs=2,
        metavar=('TYPE', 'PATH'),
        type=str,
    )
    parser.add_argument(
        '-db', '--dbms',
        help="""
        Add a database as source file.
        This arguments takes 2 position: a supported DBMS (TYPE) and the connection string (CONN).
        The DBMS requires full compatiblity with SQLAlchemy
        This arguments can be repeated.
        """,
        default=None,
        action='append',
        nargs=2,
        metavar=('TYPE', 'CONN'),
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
            arguments['datasource'].append(('file', item['type'], item['path']))
        for item in config_args['datasource']['db']:
            arguments['datasource'].append(('db', item['type'], item['conn']))

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
        for t, path in files:
            arguments['datasource'].append(('file', t, path))
    if dbs:
        for t, conn in files:
            arguments['datasource'].append(('db', t, conn))

    LOGGER.info(f'arguments: {arguments}')

    return arguments


class FerdelanceClient:

    def __init__(self, server: str, workdir: str, heartbeat: float, datasources: list[tuple[str, str, str]]) -> None:
        # possible states are: work, exit, update, install
        self.status: str = 'work'
        self.server: str = server
        self.workdir: str = workdir

        self.heartbeat: float = heartbeat

        self.private_key: RSAPrivateKey = None

        self.client_id: str = None
        self.client_token: str = None
        self.server_public_key: RSAPublicKey = None

        self.setup_completed: bool = False

        self.datasources: list[tuple[str, str, str]] = datasources

        # TODO: parse data sources

        # TODO: save config locally (and load if available in workdir)

    def add_data_source(self, type: str, connection: str):
        # TODO:
        pass

    def headers(self) -> dict[str, str]:
        """Utility method to build the headers for secure requests to the server.

        :return:
            A dictionary with the `Authorization` header.
        """
        return {
            'Authorization': f'Bearer {self.client_token}'
        }

    def create_payload(self, payload: dict) -> dict:
        """Encrypt the given payload to send to the server.

        :return:
            A dictionary with encrypted data to use in the `payload` arguments of a POST request.
        """
        return {
            'payload': encrypt(self.server_public_key, json.dumps(payload))
        }

    def get_payload(self, json_data: dict) -> dict:
        """Extract the content of a payload received from the server.

        :return:
            A decripted json from the `payload` key received from the server.
        """
        return json.loads({
            decrypt(self.private_key, json_data['payload'])
        })

    def decrypt_stream_response(self, stream: Response, out_path: str) -> None:
        with open(out_path, 'wb') as f:
            for chunk in decrypt_stream(stream.iter_content(), self.private_key):
                f.write(chunk)

    def join(self, data: dict) -> tuple[int, dict]:
        """Send a join request to the server.

        :return:
            The status code of the response, data from the response.
        """
        res = requests.post(
            f'{self.server}/client/join',
            json=data
        )

        return res.status_code, res.json()

    def leave(self) -> None:
        res = requests.post(
            '/client/leave',
            json={},
            headers=self.headers(),
        )

        ret_code = res.status_code

        if ret_code != 200:
            LOGGER.error(f'Could not leave  server {self.server}')
            return

        LOGGER.info(f'removing working directory {self.workdir}')
        shutil.rmtree(self.workdir)

        LOGGER.info(f'client left server {self.server}')

    def update(self, data: dict) -> tuple[int, str, str]:
        payload = self.create_payload(data)

        res = requests.get(
            '/client/update',
            json=payload,
            headers=self.headers(),
        )

        ret_code = res.status_code

        if ret_code != 200:
            raise ValueError(f'server response is {ret_code}')

        ret_data = self.get_payload(res.json())

        return res.status_code, ret_data['action'], ret_data['data']

    def setup(self) -> None:
        """Client initliazation (keys setup), joining the server (if not already joined), and sending metadata and sources available."""

        path_key: str = os.path.join(self.workdir, 'private_key.pem')
        path_connected: str = os.path.join(self.workdir, '.connected')
        path_server_key: str = os.path.join(self.workdir, 'server_key.pub')

        if not os.path.exists(self.workdir):
            LOGGER.info('work directory does not exists: creating folder')
            os.makedirs(self.workdir, exist_ok=True)
            os.chmod(self.workdir, 0o700)

        if not os.path.exists(path_key):
            LOGGER.info('private key does not exist: creating a new one')

            self.private_key = generate_asymmetric_key()

            with open(path_key, 'wb') as f:
                f.write(bytes_from_private_key(self.private_key))
        else:
            LOGGER.info('reading private key from disk')

            with open(path_key, 'rb') as f:
                self.private_key = private_key_from_bytes(f.read())

        if not os.path.exists(path_connected):
            LOGGER.info('collecting system info')

            public_key_bytes: bytes = bytes_from_public_key(self.private_key.public_key)

            machine_system: str = platform.system()
            machine_mac_address: str = get_mac_address()
            machine_node: str = uuid.getnode()

            LOGGER.info(f'system info: machine_system={machine_system}')
            LOGGER.info(f'system info: machine_mac_address={machine_mac_address}')
            LOGGER.info(f'system info: machine_node={machine_node}')
            LOGGER.info(f'client info: version={__version__}')

            data = {
                'system': machine_system,
                'mac_address': machine_mac_address,
                'node': machine_node,
                'public_key': b64encode(public_key_bytes).decode('utf8'),
                'version': __version__
            }

            status_code, json_data = self.join(data)

            if status_code == 403:
                LOGGER.error('client already joined, but no local files found!?')

                # TODO: what to do in this case?
                sys.exit(0)

            elif status_code == 200:
                LOGGER.info('client join sucessfull')

                self.client_id = decrypt(self.private_key, json_data['id'])
                self.client_token = decrypt(self.private_key, json_data['token'])
                self.server_public_key = public_key_from_str(decode_from_transfer(json_data['public_key']))

                with open(path_connected, 'w') as f:
                    json.dump({
                        'client_id': self.client_id,
                        'client_token': self.client_token
                    }, f)

                with open(path_server_key, 'wb') as f:
                    f.write(bytes_from_public_key(self.server_public_key))

        else:
            LOGGER.info('client already joined, reading local files')

            with open(path_connected, 'r') as f:
                json_data = json.load(f)
                self.client_id = json_data['client_id']
                self.client_token = json_data['client_token']

            with open(path_server_key, 'rb') as f:
                self.server_public_key = public_key_bytes(f.read())

        LOGGER.info('setup completed')
        self.setup_completed = True

    def run(self) -> None:
        """Main loop where the client contact the server for updates."""

        LOGGER.info('running client')

        if not self.setup_completed:
            self.setup()

        while self.status != 'exit':
            try:
                status_code, action, data = self.update({})

                # TODO: setup work loop

                if self.status == 'update':
                    LOGGER.info('update application')
                    sys.exit(1)

                if self.status == 'install':
                    LOGGER.info('update and install packages')
                    sys.exit(2)
            except ValueError as e:
                LOGGER.exception(e)
                sys.exit(0)

            LOGGER.info('waiting')
            sleep(self.heartbeat)


if __name__ == '__main__':
    load_dotenv()
    args = setup_arguments()

    client = FerdelanceClient(*args)

    if args['client']:
        client.leave()
        sys.exit(0)

    client.run()

    LOGGER.info('terminate application')
    sys.exit(0)
