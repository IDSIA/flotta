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

from dotenv import load_dotenv
from time import sleep

from base64 import b64encode
from getmac import get_mac_address

import argparse
import json
import logging
import requests
import platform
import sys
import os
import uuid

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
    parser.add_argument('-s', '--server', help='Set the url for the aggregation server', default='http://localhost/', type=str)
    parser.add_argument('-w', '--workdir', help='Set the working directory of the client', default='./FDL.client.workdir', type=str)
    parser.add_argument('-b', '--heartbeat', help='Set the amount of time in seconds to wait after a command execution.', default=1, type=float)
    return parser.parse_args()


class FerdelanceClient:

    def __init__(self, server: str, workdir: str, heartbeat: float) -> None:
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

    def join(self, data: dict) -> tuple[int, dict]:
        res = requests.post(
            f'{self.server}/client/join',
            json=data
        )

        return res.status_code, res.json()

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
                LOGGER.warn('client already joined, but no local files!')

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
            if self.status == 'update':
                LOGGER.info('update application')
                sys.exit(1)

            if self.status == 'install':
                LOGGER.info('update and install packages')
                sys.exit(2)

            LOGGER.info('waiting')
            sleep(self.heartbeat)


if __name__ == '__main__':
    load_dotenv()
    args = setup_arguments()

    client = FerdelanceClient(
        server=args.server,
        workdir=args.workdir,
        heartbeat=args.heartbeat,
    )
    client.run()

    LOGGER.info('terminate application')
    sys.exit(0)
