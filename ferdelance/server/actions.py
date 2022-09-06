from ..database.tables import Client, ClientToken
from ..database import Session, crud

from .security import generate_token

from ferdelance_shared.actions import *

from typing import Any
import logging

LOGGER = logging.getLogger(__name__)


class ActionManager:

    def _check_client_token(self, db: Session, client: Client) -> bool:
        """Checks if the token is still valid or if there is a new version.

        :return:
            True if no valid token is found, otherwise False.
        """
        n_tokens = db.query(ClientToken).filter(ClientToken.client_id == client.client_id, ClientToken.valid).count()

        LOGGER.debug(f'client_id={client.client_id}: found {n_tokens} valid token(s)')

        return n_tokens == 0

    def _action_update_token(self, db: Session, client: Client) -> tuple[str, str]:
        """Generates a new valid token.

        :return:
            The 'update_token' action and a string with the new token.
        """
        token: ClientToken = generate_token(client.machine_system, client.machine_mac_address, client.machine_node, client.client_id)
        crud.invalidate_all_tokens(db, client.client_id)
        crud.create_client_token(db, token)

        return UPDATE_TOKEN, {'token': token.token}

    def _check_client_app_update(self, db: Session, client: Client) -> bool:
        """Compares the client current version with the newest version on the database.

        :return:
            True if there is a new version and this version is different from the current client version.
        """
        version: str = crud.get_newest_app_version(db)

        LOGGER.info(f'client_id={client.client_id}: version={client.version} newest_version={version}')

        return version is not None and client.version != version

    def _action_update_client_app(self, db: Session) -> tuple[str, str]:
        """Update and restart the client with the new version.

        :return:
            Fetch and return the version to download.
        """

        new_client: str = crud.get_newest_app_version(db)

        return UPDATE_CLIENT, {
            'checksum': new_client.checksum,
            'name': new_client.name,
            'version': new_client.version,
        }

    def _action_nothing(self) -> tuple[str, Any]:
        """Do nothing and waits for the next update request."""
        return DO_NOTHING, {}

    def next(self, db: Session, client: Client, payload: str) -> tuple[str, str]:

        # TODO: consume client payload

        if self._check_client_token(db, client):
            return self._action_update_token(db, client)

        if self._check_client_app_update(db, client):
            return self._action_update_client_app(db)

        # TODO: check for tasks to do

        return self._action_nothing()
