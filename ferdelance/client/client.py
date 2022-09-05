from ferdelance import __version__
from ferdelance_shared.actions import *
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

from .datasources import DataSourceDB, DataSourceFile

from base64 import b64encode
from getmac import get_mac_address
from requests import Response
from time import sleep

import hashlib
import json
import logging
import requests
import os
import platform
import shutil
import sys
import uuid
import yaml

LOGGER = logging.getLogger(__name__)


class FerdelanceClient:

    def __init__(self, server: str, workdir: str, config: str, heartbeat: float, leave: bool, datasources: list[tuple[str, str, str, str]]) -> None:
        # possible states are: work, exit, update, install
        self.status: str = 'init'
        self.config: str = config
        self.server: str = server
        self.workdir: str = workdir

        self.heartbeat: float = heartbeat

        self.private_key: RSAPrivateKey = None

        self.client_id: str = None
        self.client_token: str = None
        self.server_public_key: RSAPublicKey = None

        self.datasources_list: list[tuple[str, str, str, str]] = datasources
        self.datasources: dict[str, DataSourceFile | DataSourceDB] = {}

        self.path_key: str = ''
        self.path_joined: str = ''
        self.path_server_key: str = ''
        self.path_config: str = ''

        self.flag_leave: bool = leave
        self.setup_completed: bool = False

    def get_datasource(self, name: str, filter: str = None) -> None:
        # TODO
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

    def decrypt_stream_response(self, stream: Response, out_path: str) -> str:
        """Decrypt an incoming stream of data using a local private key and compute the checksum.

        :param setream:
            Stream to read from.
        :param out_path:
            Path on the disk to save the payload to.
        :return:
            Checkusm of the received data.
        """
        checksum = hashlib.sha256()

        with open(out_path, 'wb') as f:
            for chunk in decrypt_stream(stream.iter_content(), self.private_key):
                checksum.update(chunk)
                f.write(chunk)

        return checksum.hexdigest()

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

        self.path_key: str = os.path.join(self.workdir, 'private_key.pem')
        self.path_joined: str = os.path.join(self.workdir, '.joined')
        self.path_server_key: str = os.path.join(self.workdir, 'server_key.pub')
        self.path_config: str = os.path.join(self.workdir, 'config.yaml')

        # check for existing working directory
        if not os.path.exists(self.workdir):
            LOGGER.info('work directory does not exists: creating folder')
            os.makedirs(self.workdir, exist_ok=True)
            os.chmod(self.workdir, 0o700)

        # check for existing private_key.pem file
        if not os.path.exists(self.path_key):
            LOGGER.info('private key does not exist: creating a new one')

            self.private_key = generate_asymmetric_key()

            with open(self.path_key, 'wb') as f:
                f.write(bytes_from_private_key(self.private_key))
        else:
            LOGGER.info('reading private key from disk')

            with open(self.path_key, 'rb') as f:
                self.private_key = private_key_from_bytes(f.read())

        # check for existing .joined file
        if not os.path.exists(self.path_joined):
            LOGGER.info('collecting system info')

            public_key_bytes: bytes = bytes_from_public_key(self.private_key.public_key())

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

                # TODO what to do in this case?
                sys.exit(0)

            elif status_code == 200:
                LOGGER.info('client join sucessfull')

                self.client_id = decrypt(self.private_key, json_data['id'])
                self.client_token = decrypt(self.private_key, json_data['token'])
                self.server_public_key = public_key_from_str(decode_from_transfer(json_data['public_key']))

                with open(self.path_server_key, 'wb') as f:
                    f.write(bytes_from_public_key(self.server_public_key))

                open(self.path_joined, 'a').close()

        else:
            LOGGER.info('client already joined, reading local files')

            with open(self.path_joined, 'r') as f:
                json_data = json.load(f)
                self.client_id = json_data['client_id']
                self.client_token = json_data['client_token']

            with open(self.path_server_key, 'rb') as f:
                self.server_public_key = public_key_bytes(f.read())

        if not os.path.exists(self.path_config):
            # setup local data sources
            for t, n, k, p in self.datasources_list:
                if t == 'file':
                    self.datasources[n] = DataSourceFile(n, k, p)
                elif t == 'db':
                    self.datasources[n] = DataSourceDB(n, k, p)
                else:
                    LOGGER.error(f'Invalid data source: TYPE={t} NAME={n} KIND={k} CONN={p}')

            # save config locally (TODO load if available in workdir)
            self.dump_config()

        LOGGER.info('setup completed')
        self.setup_completed = True

    def dump_config(self) -> None:
        """Save current configuration to a file in the working directory."""
        with open(self.path_config, 'w') as f:
            yaml.safe_dump({
                'version': __version__,

                'server': self.server,
                'workdir': self.workdir,
                'heartbeat': self.heartbeat,
                'datasources': self.datasources,

                'client_id': self.client_id,
                'client_token': self.client_token,
            }, f)

    def send_metadata(self) -> None:
        LOGGER.info('sending metadata to remote')
        # TODO: send data sources available
        # FIXME: need server endpoint!
        pass

    def perform_action(self, action: str, data: dict) -> str:
        if action == UPDATE_TOKEN:
            LOGGER.info('updating client token with a new one')
            self.client_token = data['token']
            self.dump_config()

        if action == UPDATE_CLIENT:
            version_app = data['version']
            filename = data['name']
            expected_checksum = data['checksum']

            with requests.post(
                '/client/update/files',
                json=self.create_payload({'client_version': version_app}),
                headers=self.headers(),
                stream=True,
            ) as stream:
                if stream.status_code != 200:
                    LOGGER.error(f'could not download new client version={version_app} from server={self.server}')
                    return 'update'

                path_file: str = os.path.join(self.workdir, filename)
                checksum: str = self.decrypt_stream_response(stream, path_file)

                if checksum != expected_checksum:
                    LOGGER.error('Checksum mismatch: received invalid data!')
                    return DO_NOTHING

                LOGGER.error(f'Checksum of {path_file} passed')

                with open('.update', 'w') as f:
                    f.write(path_file)

                # TODO: this is something for the next iteration

                return 'update'

        if action == DO_NOTHING:
            LOGGER.info('nothing new from the server')
            return DO_NOTHING

        LOGGER.error(f'cannot complete action={action}')
        return DO_NOTHING

    def run(self) -> None:
        """Main loop where the client contact the server for updates."""

        LOGGER.info('running client')

        if self.flag_leave:
            self.leave()

        if not self.setup_completed:
            self.setup()

        self.send_metadata()

        while self.status != 'exit':
            try:
                LOGGER.info('requesting update')
                status_code, action, data = self.update({})

                LOGGER.info(f'update: status_code={status_code} action={action}')

                # work loop
                self.status = self.perform_action(action, data)

                if self.status == 'update':
                    LOGGER.info('update application and dependencies')
                    sys.exit(1)

            except ValueError as e:
                LOGGER.exception(e)
                sys.exit(0)

            LOGGER.info('waiting')
            sleep(self.heartbeat)
