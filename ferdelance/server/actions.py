from ..database import Session

from typing import Any
import logging 

LOGGER = logging.getLogger(__name__)


class ActionManager:

    def _check_client_token(self) -> bool:

        # TODO: check if the token is still valid or if there is a new version

        return False

    def _action_update_token(self) -> tuple[str, str]:
        """Update the token with the new one"""

        # TODO: generate a new token

        return 'update_token', None

    def _check_client_update(self) -> bool:

        # TODO: compare client version with latest version

        return False

    def _action_update_client(self) -> tuple[str, str]:
        """Update and restart the client with the new version."""

        # TODO: check the table for the latest client software update, fetch and return it

        return 'update_client', None

    def _check_code_update(self) -> bool:

        # TODO: check the table for the next code to run

        return False

    def _action_update_code(self) -> tuple[str, str]:
        """Update and execute the new code."""

        # TODO: fetch the table for the next code to run, and return it

        return 'update_code', None

    def _action_nothing(self) -> tuple[str, Any]:
        """Do nothing and waits for the next update request."""
        return 'nothing', None

    def next(self, db: Session, client_id: str) -> tuple[str, str]:
        LOGGER.info(f'sending action=nothing to client_id={client_id}')

        if self._check_client_token():
            return self._action_update_token()
        
        if self._check_client_update():
            return self._action_update_client()
        
        if self._check_code_update():
            return self._action_update_code()

        return self._action_nothing()
