from typing import Any

from ferdelance.config import get_logger
from ferdelance.database.repositories import (
    AsyncSession,
    ComponentRepository,
    JobRepository,
)
from ferdelance.shared.actions import Action
from ferdelance.schemas.components import Client, Token
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.updates import (
    UpdateExecute,
    UpdateNothing,
    UpdateToken,
)

from sqlalchemy.exc import NoResultFound

LOGGER = get_logger(__name__)


class ActionService:
    def __init__(self, session: AsyncSession) -> None:
        self.session: AsyncSession = session

        self.jr: JobRepository = JobRepository(session)
        self.cr: ComponentRepository = ComponentRepository(session)

    async def _check_client_token(self, client: Client) -> bool:
        """Checks if the token is still valid or if there is a new version.

        :return:
            True if no valid token is found, otherwise False.
        """
        return await self.cr.has_invalid_token(client.id)

    async def _action_update_token(self, client: Client) -> UpdateToken:
        """Generates a new valid token.

        :return:
            The 'update_token' action and a string with the new token.
        """

        token: Token = await self.cr.update_client_token(client)

        return UpdateToken(action=Action.UPDATE_TOKEN.name, token=token.token)

    async def _check_scheduled_job(self, client: Client) -> Job:
        return await self.jr.next_job_for_component(client.id)

    async def _action_schedule_job(self, job: Job) -> UpdateExecute:
        if job.is_model:
            action = Action.EXECUTE_TRAINING
        elif job.is_estimation:
            action = Action.EXECUTE_ESTIMATE
        else:
            LOGGER.error(f"Invalid action type for job_id={job.id}")
            raise ValueError()

        return UpdateExecute(action=action.name, job_id=job.id, artifact_id=job.artifact_id)

    async def _action_nothing(self) -> UpdateNothing:
        """Do nothing and waits for the next update request."""
        return UpdateNothing(action=Action.DO_NOTHING.name)

    async def next(self, client: Client, payload: dict[str, Any]) -> UpdateExecute | UpdateNothing | UpdateToken:
        # TODO: consume client payload

        try:
            if await self._check_client_token(client):
                return await self._action_update_token(client)

            task = await self._check_scheduled_job(client)
            return await self._action_schedule_job(task)

        except NoResultFound:
            # no tasks to do
            pass

        except Exception as e:
            # real exception
            LOGGER.warn(e)

        return await self._action_nothing()
