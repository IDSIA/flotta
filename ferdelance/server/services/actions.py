from typing import Any

from ferdelance.database.repositories import (
    Repository,
    AsyncSession,
    ComponentRepository,
    JobRepository,
)
from ferdelance.database.repositories import ComponentRepository
from ferdelance.shared.actions import Action
from ferdelance.schemas.components import Client, Token, Application
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.updates import (
    UpdateClientApp,
    UpdateExecute,
    UpdateNothing,
    UpdateToken,
)

from sqlalchemy.exc import NoResultFound

import logging

LOGGER = logging.getLogger(__name__)


class ActionService(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.jr: JobRepository = JobRepository(session)
        self.cr: ComponentRepository = ComponentRepository(session)

    async def _check_client_token(self, client: Client) -> bool:
        """Checks if the token is still valid or if there is a new version.

        :return:
            True if no valid token is found, otherwise False.
        """
        return await self.cr.has_invalid_token(client.client_id)

    async def _action_update_token(self, client: Client) -> UpdateToken:
        """Generates a new valid token.

        :return:
            The 'update_token' action and a string with the new token.
        """
        cr: ComponentRepository = ComponentRepository(self.session)

        token: Token = await cr.update_client_token(client)

        return UpdateToken(action=Action.UPDATE_TOKEN.name, token=token.token)

    async def _check_client_app_update(self, client: Client) -> bool:
        """Compares the client current version with the newest version on the database.

        :return:
            True if there is a new version and this version is different from the current client version.
        """
        try:
            app: Application = await self.cr.get_newest_app()

            LOGGER.debug(f"client_id={client.client_id}: version={client.version} newest_version={app.version}")

            return client.version != app.version

        except NoResultFound:
            return False

    async def _action_update_client_app(self) -> UpdateClientApp:
        """Update and restart the client with the new version.

        :return:
            Fetch and return the version to download.
        """

        new_client: Application = await self.cr.get_newest_app()

        return UpdateClientApp(
            action=Action.UPDATE_CLIENT.name,
            checksum=new_client.checksum,
            name=new_client.name,
            version=new_client.version,
        )

    async def _check_scheduled_job(self, client: Client) -> Job:
        return await self.jr.next_job_for_component(client.client_id)

    async def _action_schedule_job(self, job: Job) -> UpdateExecute:
        if job.is_model:
            action = Action.EXECUTE_TRAINING
        elif job.is_estimation:
            action = Action.EXECUTE_ESTIMATE
        else:
            LOGGER.error(f"Invalid action type for job_id={job.job_id}")
            raise ValueError()

        return UpdateExecute(action=action.name, artifact_id=job.artifact_id)

    async def _action_nothing(self) -> UpdateNothing:
        """Do nothing and waits for the next update request."""
        return UpdateNothing(action=Action.DO_NOTHING.name)

    async def next(
        self, client: Client, payload: dict[str, Any]
    ) -> UpdateClientApp | UpdateExecute | UpdateNothing | UpdateToken:

        # TODO: consume client payload

        try:
            if await self._check_client_token(client):
                return await self._action_update_token(client)

            if await self._check_client_app_update(client):
                return await self._action_update_client_app()

            task = await self._check_scheduled_job(client)
            return await self._action_schedule_job(task)
        except Exception:
            pass

        return await self._action_nothing()
