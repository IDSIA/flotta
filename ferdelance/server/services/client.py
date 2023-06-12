from typing import Any, Callable

from ferdelance.database import AsyncSession
from ferdelance.database.repositories import (
    ComponentRepository,
    DataSourceRepository,
    ProjectRepository,
)
from ferdelance.jobs import job_manager, JobManagementService
from ferdelance.schemas.client import (
    ClientJoinRequest,
    ClientJoinData,
    ClientTask,
)
from ferdelance.schemas.components import (
    Client,
    Application,
)
from ferdelance.schemas.database import Result
from ferdelance.schemas.jobs import Job
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.updates import (
    DownloadApp,
    UpdateClientApp,
    UpdateExecute,
    UpdateNothing,
    UpdateToken,
)
from ferdelance.server.services import ActionService

from sqlalchemy.exc import NoResultFound

import logging

LOGGER = logging.getLogger(__name__)


class ClientConnectService:
    def __init__(self, session: AsyncSession) -> None:
        self.session: AsyncSession = session

    async def connect(
        self, client_public_key: str, data: ClientJoinRequest, ip_address: str
    ) -> tuple[ClientJoinData, Client]:
        """
        :raise:
            NoResultFound if the access parameters (token) is not valid.

            SLQAlchemyError if there are issues with the creation of a new user in the database.

            ValueError if the given client data are incomplete or wrong.

        :return:
            A WorkbenchJoinData object that can be returned to the connected workbench.
        """
        cr: ComponentRepository = ComponentRepository(self.session)

        try:
            await cr.get_by_key(client_public_key)

            raise ValueError("Invalid client data")

        except NoResultFound as e:
            LOGGER.info("joining new client")
            # create new client
            client, token = await cr.create_client(
                name=data.name,
                version=data.version,
                public_key=client_public_key,
                machine_system=data.system,
                machine_mac_address=data.mac_address,
                machine_node=data.node,
                ip_address=ip_address,
            )

            LOGGER.info(f"client_id={client.client_id}: created new client")

            await cr.create_event(client.client_id, "creation")

        LOGGER.info(f"client_id={client.client_id}: created new client")

        return (
            ClientJoinData(
                id=client.client_id,
                token=token.token,
                public_key="",
            ),
            client,
        )


class ClientService:
    def __init__(self, session: AsyncSession, component_id: str) -> None:
        self.session: AsyncSession = session
        self.component_id: str = component_id
        self.jm: JobManagementService = JobManagementService(session)

    async def leave(self) -> None:
        """
        :raise:
            NoResultFound when there is no project with the given token.
        """
        cr: ComponentRepository = ComponentRepository(self.session)
        await cr.client_leave(self.component_id)
        await cr.create_event(self.component_id, "left")

    async def update(self, payload: dict[str, Any]) -> UpdateClientApp | UpdateExecute | UpdateNothing | UpdateToken:
        cr: ComponentRepository = ComponentRepository(self.session)
        acs: ActionService = ActionService(self.session)

        await cr.create_event(self.component_id, "update")
        client = await cr.get_client_by_id(self.component_id)

        next_action = await acs.next(client, payload)

        LOGGER.debug(f"client_id={self.component_id}: update action={next_action.action}")

        await cr.create_event(self.component_id, f"action:{next_action.action}")

        return next_action

    async def update_files(self, payload: DownloadApp) -> Application:
        cr: ComponentRepository = ComponentRepository(self.session)

        await cr.create_event(self.component_id, "update files")

        new_app: Application = await cr.get_newest_app()

        if new_app.version != payload.version:
            LOGGER.warning(
                f"client_id={self.component_id} requested app version={payload.version} while latest version={new_app.version}"
            )
            raise ValueError("Old versions are not permitted")

        await cr.update_client(self.component_id, version=payload.version)

        LOGGER.info(f"client_id={self.component_id}: requested new client version={payload.version}")

        return new_app

    async def update_metadata(self, metadata: Metadata) -> Metadata:
        cr: ComponentRepository = ComponentRepository(self.session)
        dsr: DataSourceRepository = DataSourceRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        await cr.create_event(self.component_id, "update metadata")

        await dsr.create_or_update_from_metadata(self.component_id, metadata)  # this will also update existing metadata
        await pr.add_datasources_from_metadata(metadata)

        return metadata

    async def get_task(self, payload: UpdateExecute) -> ClientTask:
        cr: ComponentRepository = ComponentRepository(self.session)

        await cr.create_event(self.component_id, "schedule task")

        job_id = payload.job_id

        content = await self.jm.client_task_start(job_id, self.component_id)

        return content

    async def result(self, job_id: str):
        result_db = await self.jm.client_result_create(job_id, self.component_id)

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
