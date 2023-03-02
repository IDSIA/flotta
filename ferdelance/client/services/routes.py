from ferdelance.client.config import Config
from ferdelance.client.exceptions import ErrorClient
from ferdelance.shared.actions import Action
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.client import ClientJoinData, ClientJoinRequest, ClientTask
from ferdelance.schemas.updates import DownloadApp, UpdateClientApp, UpdateExecute

from requests import Session, get, post
from requests.adapters import HTTPAdapter, Retry

import json
import logging
import os
import shutil


LOGGER = logging.getLogger(__name__)


class RouteService:
    def __init__(self, config: Config) -> None:
        self.config = config

    def join(self, join_data: ClientJoinRequest) -> ClientJoinData:
        """Send a join request to the server.

        :param system:
            Operative system type.
        :param mac_address:
            Machine's network interface card address.
        :param node:
            Unique node for machine.
        :param encoded_public_key:
            Component public key encoded in base64.
        :param version:
            Current client version.
        :return:
            The connection data for a join request.
        """

        s = Session()

        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])

        s.mount("http://", HTTPAdapter(max_retries=retries))

        res = s.post(f"{self.config.server}/client/join", data=json.dumps(join_data.dict()))

        res.raise_for_status()

        return ClientJoinData(**self.config.exc.get_payload(res.content))

    def leave(self) -> None:
        res = post(
            f"{self.config.server}/client/leave",
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        LOGGER.info(f"removing working directory {self.config.workdir}")
        shutil.rmtree(self.config.workdir)

        LOGGER.info(f"client left server {self.config.server}")
        raise ErrorClient()

    def send_metadata(self) -> None:
        LOGGER.info("sending metadata to remote")

        metadata: Metadata = Metadata(datasources=[ds.metadata() for _, ds in self.config.datasources.items()])

        res = post(
            f"{self.config.server}/client/update/metadata",
            data=self.config.exc.create_payload(metadata.dict()),
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        LOGGER.info("metadata uploaded successful")

        # return metadata

    def get_update(self, content: dict) -> tuple[Action, dict]:
        LOGGER.debug("requesting update")

        res = get(
            f"{self.config.server}/client/update",
            data=self.config.exc.create_payload(content),
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        data = self.config.exc.get_payload(res.content)

        return Action[data["action"]], data

    def get_task(self, task: UpdateExecute) -> ClientTask:
        LOGGER.info("requesting new client task")

        res = get(
            f"{self.config.server}/client/task",
            data=self.config.exc.create_payload(task.dict()),
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        return ClientTask(**self.config.exc.get_payload(res.content))

    def get_new_client(self, data: UpdateClientApp):
        expected_checksum = data.checksum
        download_app = DownloadApp(name=data.name, version=data.version)

        with post(
            f"{self.config.server}/client/download/application",
            data=self.config.exc.create_payload(download_app.dict()),
            headers=self.config.exc.headers(),
            stream=True,
        ) as stream:
            if not stream.ok:
                LOGGER.error(f"could not download new client version={data.version} from server={self.config.server}")
                return "update"

            path_file: str = os.path.join(self.config.workdir, data.name)
            checksum: str = self.config.exc.stream_response_to_file(stream, path_file)

            if checksum != expected_checksum:
                LOGGER.error("Checksum mismatch: received invalid data!")
                return Action.DO_NOTHING

            LOGGER.error(f"Checksum of {path_file} passed")

            with open(".update", "w") as f:
                f.write(path_file)

    def post_result(self, artifact_id: str, path_in: str):
        path_out = f"{path_in}.enc"

        self.config.exc.encrypt_file_for_remote(path_in, path_out)

        res = post(
            f"{self.config.server}/client/result/{artifact_id}",
            data=open(path_out, "rb"),
            headers=self.config.exc.headers(),
        )

        if os.path.exists(path_out):
            os.remove(path_out)

        res.raise_for_status()

        LOGGER.info(f"artifact_id={artifact_id}: model from source={path_in} upload successful")

    def post_metrics(self, metrics: Metrics):
        res = post(
            f"{self.config.server}/client/metrics",
            data=self.config.exc.create_payload(metrics.dict()),
            headers=self.config.exc.headers(),
        )

        res.raise_for_status()

        LOGGER.info(f"artifact_id={metrics.artifact_id}: metrics from source={metrics.source} upload successful")
