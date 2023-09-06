from ferdelance import __version__
from ferdelance.client.state import State
from ferdelance.client.exceptions import ErrorClient
from ferdelance.const import TYPE_CLIENT
from ferdelance.logging import get_logger
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.node import JoinData, NodeJoinRequest
from ferdelance.shared.actions import Action
from ferdelance.shared.checksums import str_checksum
from ferdelance.shared.exchange import Exchange
from ferdelance.utils import check_url

from requests import get, post

import json
import shutil
import uuid


LOGGER = get_logger(__name__)


class RouteService:
    def __init__(self, state: State) -> None:
        self.state: State = state
        self.exc: Exchange = Exchange()

        if self.state.private_key_location is not None:
            self.exc.load_key(self.state.private_key_location)

        if self.state.node_public_key is not None:
            self.exc.set_remote_key(self.state.node_public_key)

    def check(self) -> None:
        """Checks that the server node is up, if not wait for a little bit."""
        check_url(f"{self.state.server}/client/")

    def join(self) -> JoinData:
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

        self.state.client_id = str(uuid.uuid4())

        public_key = self.exc.transfer_public_key()

        data_to_sign = f"{self.state.client_id}:{public_key}"

        checksum = str_checksum(data_to_sign)
        signature = self.exc.sign(data_to_sign)

        join_data = NodeJoinRequest(
            id=self.state.client_id,
            name=self.state.name,
            type_name=TYPE_CLIENT,
            public_key=public_key,
            version=__version__,
            url="",
            checksum=checksum,
            signature=signature,
        )

        _, payload = self.exc.create_payload(join_data.json())
        headers = self.exc.create_header(True)

        res = post(
            f"{self.state.server}/node/join",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, payload = self.exc.get_payload(res.content)

        return JoinData(**json.loads(payload))

    def leave(self) -> None:
        """Send a leave request to the server."""

        headers, payload = self.exc.create(self.state.client_id, "", True)

        res = post(
            f"{self.state.server}/node/leave",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        LOGGER.info(f"removing working directory {self.state.workdir}")
        shutil.rmtree(self.state.workdir)

        LOGGER.info(f"client left server {self.state.server}")
        raise ErrorClient()

    def send_metadata(self) -> None:
        LOGGER.info("sending metadata to remote")

        metadata: Metadata = self.state.data.metadata()
        headers, payload = self.exc.create(self.state.client_id, metadata.json())

        res = post(
            f"{self.state.server}/node/metadata",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        LOGGER.info("metadata uploaded successful")

    def get_update(self, content: dict) -> tuple[Action, dict]:
        """Heartbeat command to check for an update from the server."""
        LOGGER.debug("requesting update")

        headers, payload = self.exc.create(self.state.client_id, json.dumps(content))

        res = get(
            f"{self.state.server}/client/update",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, res_payload = self.exc.get_payload(res.content)

        data = json.loads(res_payload)

        return Action[data["action"]], data
