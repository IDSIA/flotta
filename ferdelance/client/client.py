from ferdelance import __version__
from ferdelance_shared.actions import *
from ferdelance_shared.generate import (
    generate_asymmetric_key,
    bytes_from_private_key,
    bytes_from_public_key,
    private_key_from_bytes,
    public_key_from_str,
    public_key_from_bytes,
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
import stat
import sys
import uuid
import yaml

LOGGER = logging.getLogger(__name__)


class SetupError(Exception):
    pass


class FerdelanceClient:

    def __init__(self, server: str = 'localhost', workdir: str = 'workdir', heartbeat: float = 1.0, leave: bool = False, datasources: list[tuple[str, str, str, str]] = dict()) -> None:
        # possible states are: work, exit, update, install
        self.status: str = 'init'
        self.server: str = server.rstrip('/')
        self.workdir: str = workdir

        self.heartbeat: float = heartbeat

        self.private_key: RSAPrivateKey = None

        self.client_id: str = None
        self.client_token: str = None
        self.server_public_key: RSAPublicKey = None

        self.datasources_list: list[tuple[str, str, str, str]] = datasources
        self.datasources: dict[str, DataSourceFile | DataSourceDB] = {}

        self.path_joined: str = os.path.join(self.workdir, '.joined')
        self.path_properties: str = os.path.join(self.workdir, 'properties.yaml')
        self.path_server_key: str = os.path.join(self.workdir, 'server_key.pub')
        self.path_private_key: str = os.path.join(self.workdir, 'private_key.pem')

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
        return json.loads(decrypt(self.private_key, json_data['payload']))

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

    def request_join(self, data: dict) -> tuple[int, dict]:
        """Send a join request to the server.

        :return:
            The status code of the response, data from the response.
        """
        res = requests.post(
            f'{self.server}/client/join',
            json=data
        )

        return res.status_code, res.json()

    def request_leave(self) -> None:
        res = requests.post(
            f'{self.server}/client/leave',
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

    def request_update(self, data: dict) -> tuple[int, str, str]:
        payload = self.create_payload(data)

        res = requests.get(
            f'{self.server}/client/update',
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
        LOGGER.info('client initialization')

        what_is_missing = []

        try:
            # check for existing working directory
            if os.path.exists(self.workdir):
                LOGGER.info(f'loading properties from working directory={self.workdir}')

                status = os.stat(self.workdir)
                chmod = stat.S_IMODE(status.st_mode & 0o777)

                if chmod != 0o700:
                    LOGGER.error(f'working directory {self.workdir} has wrong permissions!')
                    LOGGER.error(f'expected {0o700} found {chmod}')
                    sys.exit(2)

                if os.path.exists(self.path_properties):
                    # load properties
                    LOGGER.info(f'loading properties file from {self.path_properties}')
                    with open(self.path_properties, 'r') as f:
                        props = yaml.safe_load(f)

                        self.client_id = props['client_id']
                        self.client_token = props['client_token']
                        self.server = props['server']

                        self.heartbeat = props['heartbeat']

                        # TODO: load data sources

                        self.path_joined = props['path_joined']
                        self.path_server_key = props['path_server_key']
                        self.path_private_key = props['path_private_key']

                if os.path.exists(self.path_private_key):
                    with open(self.path_private_key, 'rb') as f:
                        data = f.read()
                        self.private_key = private_key_from_bytes(data)
                else:
                    LOGGER.info(f'private key not found at {self.path_private_key}')
                    what_is_missing.append('pk')
                    what_is_missing.append('join')
                    raise SetupError()

                if os.path.exists(self.path_joined):
                    # already joined

                    if os.path.exists(self.path_server_key):
                        LOGGER.info(f'reading server key from {self.path_server_key}')
                        with open(self.path_server_key, 'rb') as f:
                            data = f.read()
                            self.server_public_key = public_key_from_bytes(data)
                    else:
                        LOGGER.info(f'reading server key not found at {self.path_server_key}')
                        what_is_missing.append('join')
                        raise SetupError()

                    if self.client_id is None or self.client_token is None or not os.path.exists(self.path_joined):
                        LOGGER.info(f'client not joined')
                        what_is_missing.append('join')
                        raise SetupError()

                else:
                    LOGGER.info(f'client not joined')
                    what_is_missing.append('join')
                    raise SetupError()

            else:
                # empty directory
                LOGGER.info('working directory does not exists')
                what_is_missing.append('wd')
                what_is_missing.append('pk')
                what_is_missing.append('join')
                raise SetupError()

        except SetupError as _:
            LOGGER.info(f'missing setup files: {what_is_missing}')

            for item in what_is_missing:

                if item == 'wd':
                    LOGGER.info(f'creating working directory={self.workdir}')
                    os.makedirs(self.workdir, exist_ok=True)
                    os.chmod(self.workdir, 0o700)

                if item == 'pk':
                    LOGGER.info('private key does not exist: creating a new one')
                    self.private_key = generate_asymmetric_key()

                    with open(self.path_private_key, 'wb') as f:
                        f.write(bytes_from_private_key(self.private_key))

                if item == 'join':
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

                    status_code, json_data = self.request_join(data)

                    if status_code == 403:
                        LOGGER.error('client already joined, but no local files found!?')

                        # TODO what to do in this case?
                        sys.exit(2)

                    elif status_code == 200:
                        LOGGER.info('client join sucessfull')

                        self.client_id = decrypt(self.private_key, json_data['id'])
                        self.client_token = decrypt(self.private_key, json_data['token'])
                        self.server_public_key = public_key_from_str(decode_from_transfer(json_data['public_key']))

                        with open(self.path_server_key, 'wb') as f:
                            f.write(bytes_from_public_key(self.server_public_key))

                        open(self.path_joined, 'a').close()

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

        # TODO: load data sources

        LOGGER.info('setup completed')
        self.setup_completed = True

    def dump_config(self) -> None:
        """Save current configuration to a file in the working directory."""
        with open(self.path_properties, 'w') as f:
            yaml.safe_dump({
                'version': __version__,

                'server': self.server,
                'workdir': self.workdir,
                'heartbeat': self.heartbeat,
                'datasources': self.datasources,

                'client_id': self.client_id,
                'client_token': self.client_token,

                'path_joined': self.path_joined,
                'path_server_key': self.path_server_key,
                'path_private_key': self.path_private_key,
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
        try:
            LOGGER.info('running client')

            if self.flag_leave:
                self.request_leave()

            if not self.setup_completed:
                self.setup()

            self.send_metadata()

            while self.status != 'exit':
                try:
                    LOGGER.info('requesting update')
                    status_code, action, data = self.request_update({})

                    LOGGER.info(f'update: status_code={status_code} action={action}')

                    # work loop
                    self.status = self.perform_action(action, data)

                    if self.status == 'update':
                        LOGGER.info('update application and dependencies')
                        sys.exit(1)

                except ValueError as e:
                    # TODO: discriminate between bad and acceptable exceptions
                    LOGGER.exception(e)
                    sys.exit(2)

                LOGGER.info('waiting')
                sleep(self.heartbeat)

        except Exception as e:
            LOGGER.fatal(e)
            LOGGER.exception(e)
            sys.exit(2)
