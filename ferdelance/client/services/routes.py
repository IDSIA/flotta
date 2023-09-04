from ferdelance.client.state import State
from ferdelance.client.exceptions import ErrorClient
from ferdelance.logging import get_logger
from ferdelance.shared.actions import Action
from ferdelance.shared.exchange import Exchange
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.node import JoinData, NodeJoinRequest
from ferdelance.utils import check_url

from requests import get, post

import json
import shutil


LOGGER = get_logger(__name__)


class RouteService:
    def __init__(self, state: State) -> None:
        self.state: State = state
        self.exc: Exchange = Exchange()

        if self.state.private_key_location is not None:
            self.exc.load_key(self.state.private_key_location)

        if self.state.node_public_key is not None:
            self.exc.set_remote_key(self.state.node_public_key)

        if self.state.client_token is not None:
            self.exc.set_token(self.state.client_token)

    def check(self) -> None:
        """Checks that the server node is up, if not wait for a little bit."""
        check_url(f"{self.state.server}/client/")

    def join(self, join_data: NodeJoinRequest) -> JoinData:
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
        res = post(f"{self.state.server}/node/join", data=json.dumps(join_data.dict()))

        res.raise_for_status()

        return JoinData(**self.exc.get_payload(res.content))

    def leave(self) -> None:
        """Send a leave request to the server."""
        res = post(
            f"{self.state.server}/node/leave",
            headers=self.exc.headers(),
        )

        res.raise_for_status()

        LOGGER.info(f"removing working directory {self.state.workdir}")
        shutil.rmtree(self.state.workdir)

        LOGGER.info(f"client left server {self.state.server}")
        raise ErrorClient()

    def send_metadata(self) -> None:
        LOGGER.info("sending metadata to remote")

        metadata: Metadata = self.state.data.metadata()

        res = post(
            f"{self.state.server}/node/metadata",
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
            f"{self.state.server}/client/update",
            data=self.exc.create_payload(content),
            headers=self.exc.headers(),
        )

        res.raise_for_status()

        data = self.exc.get_payload(res.content)

        return Action[data["action"]], data
