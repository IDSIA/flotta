from ferdelance.config import get_logger
from ferdelance.client.state import ClientState
from ferdelance.client.exceptions import ErrorClient
from ferdelance.shared.actions import Action
from ferdelance.shared.exchange import Exchange
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.node import JoinData, JoinRequest

from requests import Session, get, post
from requests.adapters import HTTPAdapter, Retry

import json
import shutil


LOGGER = get_logger(__name__)


class RouteService:
    def __init__(self, config: ClientState) -> None:
        self.config: ClientState = config
        self.exc: Exchange = Exchange()

        if self.config.private_key_location is not None:
            self.exc.load_key(self.config.private_key_location)

        if self.config.node_public_key is not None:
            self.exc.set_remote_key(self.config.node_public_key)

        if self.config.client_token is not None:
            self.exc.set_token(self.config.client_token)

    def check(self) -> None:
        """Checks that the server is up, if not wait for a little bit."""
        s = Session()

        retries = Retry(total=10, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])

        s.mount("http://", HTTPAdapter(max_retries=retries))

        res = s.get(f"{self.config.server}/client/")

        res.raise_for_status()

    def join(self, join_data: JoinRequest) -> JoinData:
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
        res = post(f"{self.config.server}/node/join", data=json.dumps(join_data.dict()))

        res.raise_for_status()

        return JoinData(**self.exc.get_payload(res.content))

    def leave(self) -> None:
        """Send a leave request to the server."""
        res = post(
            f"{self.config.server}/node/leave",
            headers=self.exc.headers(),
        )

        res.raise_for_status()

        LOGGER.info(f"removing working directory {self.config.workdir}")
        shutil.rmtree(self.config.workdir)

        LOGGER.info(f"client left server {self.config.server}")
        raise ErrorClient()

    def send_metadata(self) -> None:
        LOGGER.info("sending metadata to remote")

        metadata: Metadata = self.config.data.metadata()

        res = post(
            f"{self.config.server}/node/metadata",
            data=self.exc.create_payload(metadata.dict()),
            headers=self.exc.headers(),
        )

        res.raise_for_status()

        LOGGER.info("metadata uploaded successful")

        # TODO: return metadata?

    def get_update(self, content: dict) -> tuple[Action, dict]:
        """Heartbeat command to check for an update from the server."""
        LOGGER.debug("requesting update")

        res = get(
            f"{self.config.server}/client/update",
            data=self.exc.create_payload(content),
            headers=self.exc.headers(),
        )

        res.raise_for_status()

        data = self.exc.get_payload(res.content)

        return Action[data["action"]], data
