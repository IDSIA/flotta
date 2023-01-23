import logging
from typing import Any

from sqlalchemy import func, select

from ferdelance.database.schemas import Component, Token
from ferdelance.database.services import (
    ApplicationService,
    AsyncSession,
    ComponentService,
    DBSessionService,
    JobService,
)
from ferdelance.database.tables import Application, Job
from ferdelance.database.tables import Token as TokenDB
from ferdelance.shared.actions import Action
from ferdelance.shared.schemas import UpdateClientApp, UpdateExecute, UpdateNothing, UpdateToken

LOGGER = logging.getLogger(__name__)


class ActionService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.js: JobService = JobService(session)
        self.cas: ApplicationService = ApplicationService(session)
        self.cs: ComponentService = ComponentService(session)

    async def _check_client_token(self, client: Component) -> bool:
        """Checks if the token is still valid or if there is a new version.

        :return:
            True if no valid token is found, otherwise False.
        """
        n_tokens = await self.session.scalar(
            select(func.count()).select_from(TokenDB).where(TokenDB.component_id == client.client_id, TokenDB.valid)
        )

        LOGGER.debug(f"client_id={client.client_id}: found {n_tokens} valid token(s)")

        return n_tokens == 0

    async def _action_update_token(self, client: Component) -> UpdateToken:
        """Generates a new valid token.

        :return:
            The 'update_token' action and a string with the new token.
        """
        cs: ComponentService = ComponentService(self.session)

        token: Token = await cs.update_client_token(client)

        return UpdateToken(action=Action.UPDATE_TOKEN.name, token=token.token)

    async def _check_client_app_update(self, client: Component) -> bool:
        """Compares the client current version with the newest version on the database.

        :return:
            True if there is a new version and this version is different from the current client version.
        """
        app: Application | None = await self.cas.get_newest_app()

        if app is None:
            return False

        LOGGER.debug(f"client_id={client.client_id}: version={client.version} newest_version={app.version}")

        return client.version != app.version

    async def _action_update_client_app(self) -> UpdateClientApp:
        """Update and restart the client with the new version.

        :return:
            Fetch and return the version to download.
        """

        new_client: Application | None = await self.cas.get_newest_app()

        assert new_client is not None

        return UpdateClientApp(
            action=Action.UPDATE_CLIENT.name,
            checksum=new_client.checksum,
            name=new_client.name,
            version=new_client.version,
        )

    async def _check_scheduled_job(self, client: Component) -> Job | None:
        return await self.js.next_job_for_client(client.client_id)

    async def _action_schedule_job(self, job: Job) -> UpdateExecute:
        return UpdateExecute(action=Action.EXECUTE.name, artifact_id=job.artifact_id)

    async def _action_nothing(self) -> UpdateNothing:
        """Do nothing and waits for the next update request."""
        return UpdateNothing(action=Action.DO_NOTHING.name)

    async def next(
        self, client: Component, payload: dict[str, Any]
    ) -> UpdateClientApp | UpdateExecute | UpdateNothing | UpdateToken:

        # TODO: consume client payload

        if await self._check_client_token(client):
            return await self._action_update_token(client)

        if await self._check_client_app_update(client):
            return await self._action_update_client_app()

        task = await self._check_scheduled_job(client)
        if task is not None:
            return await self._action_schedule_job(task)

        return await self._action_nothing()
