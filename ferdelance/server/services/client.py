from typing import Any, Callable

from ferdelance.database import AsyncSession
from ferdelance.database.repositories import ComponentRepository
from ferdelance.jobs import job_manager, JobManagementService
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.components import Application, Component
from ferdelance.schemas.database import Result
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.updates import (
    DownloadApp,
    UpdateClientApp,
    UpdateExecute,
    UpdateNothing,
    UpdateToken,
)
from ferdelance.server.services import ActionService

import logging

LOGGER = logging.getLogger(__name__)


class ClientService:
    def __init__(self, session: AsyncSession, component: Component) -> None:
        self.session: AsyncSession = session
        self.component: Component = component
        self.jm: JobManagementService = JobManagementService(session)

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
                f"client_id={self.component.id} requested app version={payload.version} while latest version={new_app.version}"
            )
            raise ValueError("Old versions are not permitted")

        await cr.update_client(self.component.id, version=payload.version)

        LOGGER.info(f"client_id={self.component.id}: requested new client version={payload.version}")

        return new_app

    async def get_task(self, payload: UpdateExecute) -> ClientTask:
        cr: ComponentRepository = ComponentRepository(self.session)

        await cr.create_event(self.component.id, "schedule task")

        job_id = payload.job_id

        content = await self.jm.client_task_start(job_id, self.component.id)

        return content

    async def result(self, job_id: str):
        result_db = await self.jm.client_result_create(job_id, self.component.id)

        return result_db

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
            await self.start_aggregation(result, self.jm._start_aggregation)

    async def check(self, resultd: Result) -> bool:
        return await self.jm.check_for_aggregation(resultd)

    async def start_aggregation(self, result: Result, start_function: Callable[[str, str, list[str], str], str]) -> Job:
        return await self.jm.start_aggregation(result, start_function)

    async def metrics(self, metrics: Metrics) -> None:
        jm: JobManagementService = job_manager(self.session)
        await jm.save_metrics(metrics)
