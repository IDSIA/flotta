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
    HybridEncrypter,
)

from .datasources import DataSourceDB, DataSourceFile
from .utils import NumpyEncoder

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

    def __init__(self, server: str = 'http://localhost:8080', workdir: str = 'workdir', heartbeat: float | None = None, leave: bool = False, datasources: list[dict[str, str]] = dict()) -> None:
        # possible states are: work, exit, update, install
        self.status: str = 'init'
        self.server: str = server.rstrip('/')
        self.workdir: str = workdir

        self.heartbeat: float = heartbeat

        self.private_key: RSAPrivateKey = None

        self.client_id: str = None
        self.client_token: str = None
        self.server_public_key: RSAPublicKey = None

        self.datasources_list: list[dict[str, str]] = datasources
        self.datasources: dict[str, DataSourceFile | DataSourceDB] = {}

        self.path_joined: str = os.path.join(self.workdir, '.joined')
        self.path_properties: str = os.path.join(self.workdir, 'properties.yaml')
        self.path_server_key: str = os.path.join(self.workdir, 'server_key.pub')
        self.path_private_key: str = os.path.join(self.workdir, 'private_key.pem')
        self.path_artifact_folder: str = os.path.join(self.workdir, 'artifacts')

        self.flag_leave: bool = leave
        self.setup_completed: bool = False
        self.stop: bool = False

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

        :param stream:
            Stream to read from.
        :param out_path:
            Path on the disk to save the payload to.
        :return:
            Checksum of the received data.
        """
        checksum = hashlib.sha256()

        with open(out_path, 'wb') as f:
            for chunk in decrypt_stream(stream.iter_content(), self.private_key):
                checksum.update(chunk)
                f.write(chunk)

        return checksum.hexdigest()

    def request_join(self, data: dict) -> dict:
        """Send a join request to the server.

        :return:
            The status code of the response, data from the response.
        """
        res = requests.post(
            f'{self.server}/client/join',
            json=data
        )

        res.raise_for_status()

        return res.json()

    def request_leave(self) -> None:
        res = requests.post(
            f'{self.server}/client/leave',
            json={},
            headers=self.headers(),
        )

        res.raise_for_status()

        LOGGER.info(f'removing working directory {self.workdir}')
        shutil.rmtree(self.workdir)

        LOGGER.info(f'client left server {self.server}')
        sys.exit(2)

    def request_metadata(self) -> None:
        LOGGER.info('sending metadata to remote')

        ds_content = {'datasources': [ds.metadata() for _, ds in self.datasources.items()]}

        enc = HybridEncrypter(self.server_public_key)
        metadata = bytearray()
        metadata += enc.start()
        metadata += enc.update(json.dumps(ds_content, cls=NumpyEncoder))
        metadata += enc.end()

        res = requests.post(
            f'{self.server}/client/update/metadata',
            files={'file': ('metadata.bin', metadata)},
            headers=self.headers(),
        )

        res.raise_for_status()

        LOGGER.info('metadata uploaded successfull')

    def request_update(self, data: dict) -> tuple[str, str]:
        LOGGER.info('requesting update')

        payload = self.create_payload(data)

        res = requests.get(
            f'{self.server}/client/update',
            json=payload,
            headers=self.headers(),
        )

        res.raise_for_status()

        ret_data = self.get_payload(res.json())

        return ret_data['action'], ret_data['data']

    def request_client_task(self, data: dict) -> None:
        LOGGER.info('requesting new client task')
        payload = self.create_payload(data)

        with requests.get(
            f'{self.server}/client/task',
            json=payload,
            headers=self.headers(),
            stream=True,
        ) as task_response:
            content, _ = self.decrypt_stream_response(task_response, self.private_key)
            return json.loads(content)

    def setup(self) -> None:
        """Client initliazation (keys setup), joining the server (if not already joined), and sending metadata and sources available."""
        LOGGER.info('client initialization')

        what_is_missing = []

        try:
            # check for existing working directory
            if os.path.exists(self.workdir):
                LOGGER.info(f'loading properties from working directory={self.workdir}')

                # TODO: check how to enable permission check with docker
                # status = os.stat(self.workdir)
                # chmod = stat.S_IMODE(status.st_mode & 0o777)

                # if chmod != 0o700:
                #     LOGGER.error(f'working directory {self.workdir} has wrong permissions!')
                #     LOGGER.error(f'expected {0o700} found {chmod}')
                #     sys.exit(2)

                if os.path.exists(self.path_properties):
                    # load properties
                    LOGGER.info(f'loading properties file from {self.path_properties}')
                    with open(self.path_properties, 'r') as f:
                        props = yaml.safe_load(f)

                        if not props:
                            raise SetupError()

                        self.client_id = props['client_id']
                        self.client_token = props['client_token']
                        self.server = props['server']

                        if self.heartbeat == None:
                            self.heartbeat = props['heartbeat']
                        if self.heartbeat == None:
                            self.heartbeat = 1.0

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

                    os.makedirs(self.path_artifact_folder, exist_ok=True)

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

                    try:
                        json_data = self.request_join(data)

                        LOGGER.info('client join sucessfull')

                        self.client_id = decrypt(self.private_key, json_data['id'])
                        self.client_token = decrypt(self.private_key, json_data['token'])
                        self.server_public_key = public_key_from_str(decode_from_transfer(json_data['public_key']))

                        with open(self.path_server_key, 'wb') as f:
                            f.write(bytes_from_public_key(self.server_public_key))

                        open(self.path_joined, 'a').close()
                    except requests.HTTPError as e:

                        if e.response.status_code == 404:
                            LOGGER.error(f'remote server {self.server} not found.')
                            LOGGER.error(f'Waiting {self.heartbeat} second(s) and retrying')
                            sleep(self.heartbeat)
                            sys.exit(0)

                        if e.response.status_code == 403:
                            LOGGER.error('client already joined, but no local files found!?')
                            sys.exit(2)

                        if e.response.status_code == 500:
                            LOGGER.exception(e)
                            sys.exit(2)

                    except requests.exceptions.RequestException as e:
                        LOGGER.error('connection refuesd')
                        LOGGER.exception(e)
                        LOGGER.error(f'Waiting {self.heartbeat} second(s) and retrying')
                        sleep(self.heartbeat)
                        sys.exit(0)

                    except Exception as e:
                        LOGGER.error('internal error')
                        LOGGER.exception(e)
                        sys.exit(0)

        # setup local data sources
        for k, n, t, p in self.datasources_list:
            if k == 'file':
                self.datasources[n] = DataSourceFile(n, t, p)
            elif k == 'db':
                self.datasources[n] = DataSourceDB(n, t, p)
            else:
                LOGGER.error(f'Invalid data source: KIND={k} NAME={n} TYPE={t} CONN={p}')

        # save config locally
        self.dump_config()

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

                'datasources': self.datasources_list,

                'client_id': self.client_id,
                'client_token': self.client_token,

                'path_joined': self.path_joined,
                'path_server_key': self.path_server_key,
                'path_private_key': self.path_private_key,
            }, f)

    def action_update_token(self, data: dict) -> None:
        LOGGER.info('updating client token with a new one')
        self.client_token = data['token']
        self.dump_config()

    def action_update_client(self, data: dict) -> str:
        version_app = data['version']
        filename = data['name']
        expected_checksum = data['checksum']

        with requests.post(
            '/client/update/files',
            json=self.create_payload({'client_version': version_app}),
            headers=self.headers(),
            stream=True,
        ) as stream:
            if not stream.ok:
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

    def action_do_nothing(self) -> str:
        LOGGER.info('nothing new from the server')
        return DO_NOTHING

    def action_execute_task(self, data: dict) -> str:
        LOGGER.info('executing new task')
        content = self.request_client_task(data)

        # TODO: this is an example, execute required task when implemented
        artifact_id: str = content['artifact_id']

        LOGGER.info(f'received artifact_id={artifact_id}')

        with open(os.path.join(self.workdir, 'artifacts', f'{artifact_id}.json'), 'w') as f:
            json.dump(f)

    def perform_action(self, action: str, data: dict) -> str:
        LOGGER.info(f'action received={action}')

        if action == UPDATE_TOKEN:
            self.action_update_token(data)

        if action == EXEC:
            self.action_execute_task(data)

        if action == UPDATE_CLIENT:
            return self.action_update_client(data)

        if action == DO_NOTHING:
            return self.action_do_nothing()

        LOGGER.error(f'cannot complete action={action}')
        return DO_NOTHING

    def stop_loop(self):
        LOGGER.info('stopping application')
        self.stop = True

    def run(self) -> None:
        """Main loop where the client contact the server for updates."""
        try:
            LOGGER.info('running client')

            if not self.setup_completed:
                self.setup()

            if self.flag_leave:
                self.request_leave()

            self.request_metadata()

            while self.status != 'exit' and not self.stop:
                try:
                    LOGGER.info('requesting update')

                    action, data = self.request_update({})

                    LOGGER.info(f'update: action={action}')

                    # work loop
                    self.status = self.perform_action(action, data)

                    if self.status == 'update':
                        LOGGER.info('update application and dependencies')
                        sys.exit(1)

                except ValueError as e:
                    # TODO: discriminate between bad and acceptable exceptions
                    LOGGER.exception(e)

                except requests.HTTPError as e:
                    LOGGER.exception(e)
                    # TODO what to do in this case?

                except requests.exceptions.RequestException as e:
                    LOGGER.error('connection refused')
                    LOGGER.exception(e)
                    # TODO what to do in this case?

                except Exception as e:
                    LOGGER.error('internal error')
                    LOGGER.exception(e)

                    # TODO what to do in this case?
                    sys.exit(2)

                LOGGER.info(f'waiting for {self.heartbeat}')
                sleep(self.heartbeat)

        except SetupError as e:
            LOGGER.error('could not complete setup')
            LOGGER.exception(e)
            sys.exit(2)

        except Exception as e:
            LOGGER.error('Unknown error')
            LOGGER.exception(e)
            sys.exit(2)

        if self.stop:
            sys.exit(2)
