from ...database.tables import Client, ClientToken, ClientTask
from . import DBSessionService, Session
from .application import ClientAppService
from .client import ClientService
from .ctask import ClientTaskService
from .security import SecurityService

from ferdelance_shared.actions import *
from ferdelance_shared.schemas import UpdateClientApp, UpdateExecute, UpdateNothing, UpdateToken

from typing import Any
import logging

LOGGER = logging.getLogger(__name__)


class ActionService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

        self.cts: ClientTaskService = ClientTaskService(db)
        self.cas: ClientAppService = ClientAppService(db)
        self.cs: ClientService = ClientService(db)

    def _check_client_token(self, client: Client) -> bool:
        """Checks if the token is still valid or if there is a new version.

        :return:
            True if no valid token is found, otherwise False.
        """
        n_tokens = self.db.query(ClientToken).filter(ClientToken.client_id == client.client_id, ClientToken.valid).count()

        LOGGER.debug(f'client_id={client.client_id}: found {n_tokens} valid token(s)')

        return n_tokens == 0

    def _action_update_token(self, client: Client) -> UpdateToken:
        """Generates a new valid token.

        :return:
            The 'update_token' action and a string with the new token.
        """
        ss: SecurityService = SecurityService(self.db, None)
        ss.client = client
        token: ClientToken = ss.generate_token(client.machine_system, client.machine_mac_address, client.machine_node, client.client_id)
        self.cs.invalidate_all_tokens(client.client_id)
        self.cs.create_client_token(token)

        return UpdateToken(
            action=UPDATE_TOKEN,
            token=token.token
        )

    def _check_client_app_update(self, client: Client) -> bool:
        """Compares the client current version with the newest version on the database.

        :return:
            True if there is a new version and this version is different from the current client version.
        """
        version: str = self.cas.get_newest_app_version()

        LOGGER.info(f'client_id={client.client_id}: version={client.version} newest_version={version}')

        return version is not None and client.version != version

    def _action_update_client_app(self) -> UpdateClientApp:
        """Update and restart the client with the new version.

        :return:
            Fetch and return the version to download.
        """

        new_client: str = self.cas.get_newest_app_version()

        return UpdateClientApp(
            action=UPDATE_CLIENT,
            checksum=new_client.checksum,
            name=new_client.name,
            version=new_client.version,
        )

    def _check_scheduled_task(self, client: Client) -> ClientTask | None:
        return self.cts.get_next_task_for_client(client)

    def _action_schedule_task(self, task: ClientTask) -> UpdateNothing:
        return UpdateExecute(
            action=EXECUTE,
            client_task_id=task.client_task_id,
        )

    def _action_nothing(self) -> tuple[str, Any]:
        """Do nothing and waits for the next update request."""
        return UpdateNothing(
            action=DO_NOTHING
        )

    def next(self, client: Client, payload: dict[str, Any]) -> UpdateClientApp | UpdateExecute | UpdateNothing | UpdateToken:

        # TODO: consume client payload

        if self._check_client_token(client):
            return self._action_update_token(client)

        if self._check_client_app_update(client):
            return self._action_update_client_app()

        task = self._check_scheduled_task(client)
        if task is not None:
            return self._action_schedule_task(task)

        return self._action_nothing()
