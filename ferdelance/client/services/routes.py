from ferdelance_shared.encode import HybridEncrypter
from ferdelance_shared.decode import HybridDecrypter
from ferdelance_shared.actions import Action
from ferdelance_shared.schemas import *

from requests import Response, post, get

from ..config import Config

import logging
import json
import os
import shutil
import sys

LOGGER = logging.getLogger(__name__)


class RouteService:

    def __init__(self, config: Config) -> None:
        self.config = config

    def headers(self) -> dict[str, str]:
        """Utility method to build the headers for secure requests to the server.

        :return:
            A dictionary with the `Authorization` header.
        """
        return {
            'Authorization': f'Bearer {self.config.client_token}'
        }

    def create_payload(self, content: dict) -> bytes:
        """Encrypt the given payload to send to the server.

        :return:
            A dictionary with encrypted data to use in the `payload` arguments of a POST request.
        """
        print('pk', self.config.server_public_key)
        data = json.dumps(content)
        enc = HybridEncrypter(self.config.server_public_key)
        print('enc obj', enc)
        return enc.encrypt(data)

    def get_payload(self, content: str) -> dict:
        """Extract the content of a payload received from the server.

        :return:
            A decrypted json from the `payload` key received from the server.
        """
        return json.loads(HybridDecrypter(self.config.private_key).decrypt(content))

    def decrypt_stream_to_file(self, stream: Response, out_path: str) -> str:
        """Decrypt an incoming stream of data using a local private key, computes the checksum and save the content to disk.

        :param stream:
            Stream to read from.
        :param out_path:
            Path on the disk to save the payload to.
        :return:
            Checksum of the received data.
        """
        dec = HybridDecrypter(self.config.private_key)

        dec.decrypt_stream_to_file(stream.iter_content(), out_path)

        return dec.get_checksum()

    def decrypt_stream_response(self, stream: Response) -> tuple[str, str]:
        """Decrypt an incoming stream of data using a local private key and computes the checksum.

        :param stream:
            Stream to read from.
        :return:
            A tuple containing the decrypted data and a checksum of the received data.
        """
        dec = HybridDecrypter(self.config.private_key)
        data = dec.decrypt_stream(stream.iter_content())
        return data, dec.get_checksum()

    def join(self, system: str, mac_address: str, node: str, encoded_public_key: str, version: str) -> ClientJoinData:
        """Send a join request to the server.

        :param system:
            Operative system type.
        :param mac_address:
            Machine's network interface card address.
        :param node:
            Unique node for machine.
        :param encoded_public_key:
            Client public key encoded in base64.
        :param version:
            Current client version.
        :return:
            The connection data for a join request.
        """

        join_data = ClientJoinRequest(
            system=system,
            mac_address=mac_address,
            node=node,
            public_key=encoded_public_key,
            version=version,
        )

        res = post(
            f'{self.config.server}/client/join',
            data=json.dumps(join_data.dict())
        )

        res.raise_for_status()

        return ClientJoinData(**self.get_payload(res.content))

    def leave(self) -> None:
        res = post(
            f'{self.config.server}/client/leave',
            headers=self.headers(),
        )

        res.raise_for_status()

        LOGGER.info(f'removing working directory {self.config.workdir}')
        shutil.rmtree(self.config.workdir)

        LOGGER.info(f'client left server {self.server}')
        sys.exit(2)

    def send_metadata(self) -> None:
        LOGGER.info('sending metadata to remote')

        metadata = Metadata(datasources=[ds.metadata() for _, ds in self.config.datasources.items()])

        res = post(
            f'{self.config.server}/client/update/metadata',
            data=self.create_payload(metadata.dict()),
            headers=self.headers(),
        )

        res.raise_for_status()

        LOGGER.info('metadata uploaded successful')

    def get_update(self, content: dict) -> tuple[Action, dict]:
        LOGGER.info('requesting update')

        res = get(
            f'{self.config.server}/client/update',
            data=self.create_payload(content),
            headers=self.headers(),
        )

        res.raise_for_status()

        data = self.get_payload(res.content)

        return Action[data['action']], data

    def get_task(self, task: UpdateExecute) -> ArtifactTask:
        LOGGER.info('requesting new client task')

        res = get(
            f'{self.server}/client/task',
            data=self.create_payload(task.dict()),
            headers=self.headers(),
        )

        res.raise_for_status()

        return ArtifactTask(**self.get_payload(res.content))

    def get_new_client(self, data: UpdateClientApp):
        expected_checksum = data.checksum
        download_app = DownloadApp(name=data.name, version=data.version)

        with post(
            f'{self.config.server}/client/download/application',
            data=self.create_payload(download_app.dict()),
            headers=self.headers(),
            stream=True,
        ) as stream:
            if not stream.ok:
                LOGGER.error(f'could not download new client version={data.version} from server={self.config.server}')
                return 'update'

            path_file: str = os.path.join(self.config.workdir, data.name)
            checksum: str = self.decrypt_stream_to_file(stream, path_file)

            if checksum != expected_checksum:
                LOGGER.error('Checksum mismatch: received invalid data!')
                return Action.DO_NOTHING

            LOGGER.error(f'Checksum of {path_file} passed')

            with open('.update', 'w') as f:
                f.write(path_file)
