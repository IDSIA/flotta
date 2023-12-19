from ferdelance.database.repositories import (
    AsyncSession,
    ComponentRepository,
    JobRepository,
)
from ferdelance.logging import get_logger
from ferdelance.shared.actions import Action
from ferdelance.schemas.components import Component
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.updates import UpdateData

from sqlalchemy.exc import NoResultFound

LOGGER = get_logger(__name__)


class ActionService:
    def __init__(self, session: AsyncSession) -> None:
        self.session: AsyncSession = session

        self.jr: JobRepository = JobRepository(session)
        self.cr: ComponentRepository = ComponentRepository(session)

    async def _check_scheduled_job(self, component: Component) -> Job:
        return await self.jr.next_job_for_component(component.id)

    async def _action_schedule_job(self, job: Job) -> UpdateData:
        return UpdateData(action=Action.EXECUTE.name, job_id=job.id, artifact_id=job.artifact_id)

    async def _action_nothing(self) -> UpdateData:
        """Do nothing and waits for the next update request."""
        return UpdateData(action=Action.DO_NOTHING.name)

    async def next(self, component: Component) -> UpdateData:
        # TODO: consume component payload

        try:
            task = await self._check_scheduled_job(component)
            return await self._action_schedule_job(task)

        except NoResultFound:
            # no tasks to do
            pass

        except Exception as e:
            # real exception
            LOGGER.warn(e)

        return await self._action_nothing()
