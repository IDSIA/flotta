from ferdelance.shared.actions import Action
from ferdelance.shared.schemas import (
    ClientJoinData,
    ClientJoinRequest,
    UpdateExecute,
    UpdateClientApp,
    DownloadApp,
)
from ferdelance.shared.artifacts import (
    Metadata,
    Artifact,
)
from ferdelance.shared.models import Metrics

from requests import post, get

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

        return ClientJoinData(**self.config.exc.get_payload(res.content))

    def leave(self) -> None:
        res = post(
            f'{self.config.server}/client/leave',
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        LOGGER.info(f'removing working directory {self.config.workdir}')
        shutil.rmtree(self.config.workdir)

        LOGGER.info(f'client left server {self.config.server}')
        sys.exit(2)

    def send_metadata(self) -> None:
        LOGGER.info('sending metadata to remote')

        metadata: Metadata = Metadata(datasources=[ds.metadata() for _, ds in self.config.datasources.items()])

        res = post(
            f'{self.config.server}/client/update/metadata',
            data=self.config.exc.create_payload(metadata.dict()),
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        LOGGER.info('metadata uploaded successful')

        # return metadata

    def get_update(self, content: dict) -> tuple[Action, dict]:
        LOGGER.info('requesting update')

        res = get(
            f'{self.config.server}/client/update',
            data=self.config.exc.create_payload(content),
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        data = self.config.exc.get_payload(res.content)

        return Action[data['action']], data

    def get_task(self, task: UpdateExecute) -> Artifact:
        LOGGER.info('requesting new client task')

        res = get(
            f'{self.config.server}/client/task',
            data=self.config.exc.create_payload(task.dict()),
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        return Artifact(**self.config.exc.get_payload(res.content))

    def get_new_client(self, data: UpdateClientApp):
        expected_checksum = data.checksum
        download_app = DownloadApp(name=data.name, version=data.version)

        with post(
            f'{self.config.server}/client/download/application',
            data=self.config.exc.create_payload(download_app.dict()),
            headers=self.config.exc.headers(),
            stream=True,
        ) as stream:
            if not stream.ok:
                LOGGER.error(f'could not download new client version={data.version} from server={self.config.server}')
                return 'update'

            path_file: str = os.path.join(self.config.workdir, data.name)
            checksum: str = self.config.exc.stream_response_to_file(stream, path_file)

            if checksum != expected_checksum:
                LOGGER.error('Checksum mismatch: received invalid data!')
                return Action.DO_NOTHING

            LOGGER.error(f'Checksum of {path_file} passed')

            with open('.update', 'w') as f:
                f.write(path_file)

    def post_model(self, artifact_id: str, path_in: str):
        path_out = f'{path_in}.enc'

        self.config.exc.encrypt_file_for_remote(path_in, path_out)

        res = post(
            f'{self.config.server}/client/task/{artifact_id}',
            data=open(path_out, 'rb'),
            headers=self.config.exc.headers(),
        )

        if os.path.exists(path_out):
            os.remove(path_out)

        res.raise_for_status()

        LOGGER.info(f'model for artifact_id={artifact_id} uploaded successful')

    def post_metrics(self, metrics: Metrics):
        res = post(
            f'{self.config.server}/client/metrics',
            data=self.config.exc.create_payload(metrics.dict()),
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        LOGGER.info(f'metrics for artifact_id={metrics.artifact_id} source={metrics.source} uploaded successful')
