from ..database.tables import Client, ClientApp, ClientToken
from ..database import Session, crud

from .security import generate_token

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

        return 'update_token', token.token

    def _check_app_update(self, db: Session, client: Client) -> bool:
        """Compares the client current version with the newest version on the database.

        :return:
            True if there is a new version and this version is different from the current client version.
        """
        version: str = crud.get_newest_app_version(db)

        LOGGER.info(f'client_id={client.client_id}: version={client.version} newest_version={version}')

        return version is not None and client.version != version

    def _action_update_app(self, db: Session) -> tuple[str, str]:
        """Update and restart the client with the new version.

        :return:
            Fetch and return the version to download.
        """

        version: str = crud.get_newest_app_version(db)

        return 'update_client', version

    def _check_job_update(self) -> bool:

        # TODO: check the table for the next code to run

        return False

    def _action_update_code(self) -> tuple[str, str]:
        """Update and execute the new code."""

        # TODO: fetch the table for the next code to run, and return it

        return 'update_code', None

    def _action_nothing(self) -> tuple[str, Any]:
        """Do nothing and waits for the next update request."""
        return 'nothing', None

    def next(self, db: Session, client: Client, payload: str) -> tuple[str, str]:

        if self._check_client_token(db, client):
            return self._action_update_token(db, client)

        if self._check_app_update(db, client):
            return self._action_update_app(db)

        if self._check_job_update():
            return self._action_update_code()

        return self._action_nothing()
