from typing import Any, Callable

from ferdelance.config import get_logger
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    AsyncSession,
)
from ferdelance.schemas.components import Application, Component
from ferdelance.schemas.database import Result
from ferdelance.schemas.errors import TaskError
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.updates import (
    DownloadApp,
    UpdateClientApp,
    UpdateExecute,
    UpdateNothing,
    UpdateToken,
)
from ferdelance.schemas.tasks import TaskArguments, TaskParameters
from ferdelance.server.services import ActionService, JobManagementService

from sqlalchemy.exc import NoResultFound

import aiofiles
import json

LOGGER = get_logger(__name__)


class ClientService:
    def __init__(self, session: AsyncSession, component: Component) -> None:
        self.session: AsyncSession = session
        self.component: Component = component
        self.jms: JobManagementService = JobManagementService(self.session)

    async def update(self, payload: dict[str, Any]) -> UpdateClientApp | UpdateExecute | UpdateNothing | UpdateToken:
        cr: ComponentRepository = ComponentRepository(self.session)
        acs: ActionService = ActionService(self.session)

        await cr.create_event(self.component.id, "update")
        client = await cr.get_client_by_id(self.component.id)

        next_action = await acs.next(client, payload)

        LOGGER.debug(f"client_id={self.component.id}: update action={next_action.action}")

        await cr.create_event(self.component.id, f"action:{next_action.action}")

        return next_action

    async def update_files(self, payload: DownloadApp) -> Application:
        cr: ComponentRepository = ComponentRepository(self.session)

        await cr.create_event(self.component.id, "update files")

        new_app: Application = await cr.get_newest_app()

        if new_app.version != payload.version:
            LOGGER.warning(
                f"client_id={self.component.id} "
                f"requested app version={payload.version} while latest version={new_app.version}"
            )
            raise ValueError("Old versions are not permitted")

        await cr.update_client(self.component.id, version=payload.version)

        LOGGER.info(f"client_id={self.component.id}: requested new client version={payload.version}")

        return new_app

    async def get_task(self, job_id: str) -> TaskParameters:
        cr: ComponentRepository = ComponentRepository(self.session)

        await cr.create_event(self.component.id, "schedule task")

        try:
            return await self.jms.client_task_start(job_id, self.component.id)

        except NoResultFound:
            LOGGER.warning(f"client_id={self.component.id}: task with job_id={job_id} does not exists")
            raise ValueError("TaskDoesNotExists")

    async def task_completed(self, job_id: str) -> Result:
        LOGGER.info(f"client_id={self.component.id}: creating results for job_id={job_id}")

        try:
            await self.jms.client_task_completed(job_id, self.component.id)

            return await self.jms.create_result(job_id, self.component.id, False)

        except NoResultFound:
            raise ValueError(f"client_id={self.component.id}: job_id={job_id} not found")

    async def task_failed(self, error: TaskError) -> Result:
        LOGGER.info(f"client_id={self.component.id}: creating results for job_id={error.job_id}")

        try:
            await self.jms.client_task_failed(error, self.component.id)

            return await self.jms.create_result(error.job_id, self.component.id, False)

        except NoResultFound:
            raise ValueError(f"client_id={self.component.id}: job_id={error.job_id} not found")

    async def check_and_start(self, result: Result) -> None:
        """This function is a check used to determine if starting the aggregation
        of an artifact or not. Conditions to start are: all jobs related to the
        current artifact (referenced in the argument result) is completed, and
        there are no errors.

        Args:
            result (Result):
                Result produced by a client.

        Raises:
            ValueError:
                If the artifact referenced by argument result does not exists.
        """

        aggregate = await self.check(result)

        if aggregate:
            await self.jms.start_aggregation(result)

    async def check(self, result: Result) -> bool:
        return await self.jms.check_for_aggregation(result)

    async def start_aggregation(self, result: Result, start_function: Callable[[TaskArguments], None]) -> Job:
        """Utility method to pass a specific start_function, used for testing
        and debugging."""
        return await self.jms._start_aggregation(result, start_function)

    async def metrics(self, metrics: Metrics) -> None:
        ar: ArtifactRepository = ArtifactRepository(self.session)

        artifact = await ar.get_artifact(metrics.artifact_id)

        if artifact is None:
            raise ValueError(f"artifact_id={metrics.artifact_id} assigned to metrics not found")

        path = await ar.storage_location(artifact.id, f"metrics_{metrics.source}.json")

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(metrics.dict())
            await f.write(content)
