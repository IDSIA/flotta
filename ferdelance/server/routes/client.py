from ferdelance.database import get_session
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.database.repositories import (
    AsyncSession,
    ComponentRepository,
    DataSourceRepository,
    ModelRepository,
    ProjectRepository,
)
from ferdelance.server.services import (
    ActionService,
    SecurityService,
    JobManagementService,
)
from ferdelance.server.security import check_token
from ferdelance.server.exceptions import ArtifactDoesNotExists, TaskDoesNotExists

from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.client import ClientJoinRequest, ClientJoinData
from ferdelance.schemas.updates import DownloadApp, UpdateExecute
from ferdelance.schemas.components import (
    Client,
    Application,
)
from ferdelance.schemas.models import Metrics
from ferdelance.shared.decode import decode_from_transfer

from fastapi import (
    APIRouter,
    Depends,
    Request,
    HTTPException,
)
from fastapi.responses import Response

from sqlalchemy.exc import SQLAlchemyError, NoResultFound
from typing import Any

import logging

LOGGER = logging.getLogger(__name__)


client_router = APIRouter()


async def check_access(component: Client = Depends(check_token)) -> Client:
    try:
        if component.type_name != TYPE_CLIENT:
            LOGGER.warning(f"client of type={component.type_name} cannot access this route")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"client_id={component.client_id} not found")
        raise HTTPException(403)


@client_router.get("/client/")
async def client_home():
    return "Client 🏠"


@client_router.post("/client/join", response_class=Response)
async def client_join(
    request: Request,
    data: ClientJoinRequest,
    session: AsyncSession = Depends(get_session),
):
    """API for new client joining."""
    LOGGER.info("new client join request")

    cr: ComponentRepository = ComponentRepository(session)
    ss: SecurityService = SecurityService(session)

    if request.client is None:
        LOGGER.warning("client not set for request?")
        raise HTTPException(400)

    ip_address = request.client.host

    try:
        client_public_key = decode_from_transfer(data.public_key)

        try:
            await cr.get_by_key(client_public_key)

            raise HTTPException(403, "Invalid client data")

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

        await ss.setup(client.public_key)

        cjd = ClientJoinData(
            id=client.client_id,
            token=token.token,
            public_key=ss.get_server_public_key(),
        )

        return ss.create_response(cjd.dict())

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception("Database error")
        raise HTTPException(500, "Internal error")

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid client data")


@client_router.post("/client/leave")
async def client_leave(
    session: AsyncSession = Depends(get_session),
    client: Client = Depends(check_access),
):
    """API for existing client to be removed"""
    cr: ComponentRepository = ComponentRepository(session)

    LOGGER.info(f"client_id={client.client_id}: request to leave")

    await cr.client_leave(client.client_id)
    await cr.create_event(client.client_id, "left")

    return {}


@client_router.get("/client/update", response_class=Response)
async def client_update(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Client = Depends(check_access),
):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new artifact package
    - new client app package
    - nothing (keep alive)
    """

    cr: ComponentRepository = ComponentRepository(session)
    acs: ActionService = ActionService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(client.public_key)
    await cr.create_event(client.client_id, "update")

    # consume current results (if present) and compute next action
    payload: dict[str, Any] = await ss.read_request(request)

    next_action = await acs.next(client, payload)

    LOGGER.debug(f"client_id={client.client_id}: update action={next_action.action}")

    await cr.create_event(client.client_id, f"action:{next_action.action}")

    return ss.create_response(next_action.dict())


@client_router.get("/client/download/application", response_class=Response)
async def client_update_files(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Client = Depends(check_access),
):
    """
    API request by the client to get updated files. With this endpoint a client can:
    - update application software
    - obtain model files
    """
    LOGGER.info(f"client_id={client.client_id}: update files request")

    cr: ComponentRepository = ComponentRepository(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(client.public_key)
    await cr.create_event(client.client_id, "update files")

    data = await ss.read_request(request)
    payload = DownloadApp(**data)

    try:
        new_app: Application = await cr.get_newest_app()

        if new_app.version != payload.version:
            LOGGER.warning(
                f"client_id={client.client_id} requested app version={payload.version} while latest version={new_app.version}"
            )
            raise HTTPException(400, "Old versions are not permitted")

        await cr.update_client(client.client_id, version=payload.version)

        LOGGER.info(f"client_id={client.client_id}: requested new client version={payload.version}")

        return ss.encrypt_file(new_app.path)
    except NoResultFound as _:
        raise HTTPException(404, "no newest version found")


@client_router.post("/client/update/metadata")
async def client_update_metadata(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Client = Depends(check_access),
):
    """Endpoint used by a client to send information regarding its metadata. These metadata includes:
    - data source available
    - summary (source, data type, min value, max value, standard deviation, ...) of features available for each data source
    """
    LOGGER.info(f"client_id={client.client_id}: update metadata request")

    cr: ComponentRepository = ComponentRepository(session)
    dsr: DataSourceRepository = DataSourceRepository(session)
    pr: ProjectRepository = ProjectRepository(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(client.public_key)
    await cr.create_event(client.client_id, "update metadata")

    data = await ss.read_request(request)
    metadata = Metadata(**data)

    await dsr.create_or_update_metadata(client.client_id, metadata)  # this will also update metadata
    await pr.add_datasources_from_metadata(metadata)

    return ss.create_response(metadata.dict())


@client_router.get("/client/task", response_class=Response)
async def client_get_task(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Client = Depends(check_access),
):
    LOGGER.info(f"client_id={client.client_id}: new task request")

    cr: ComponentRepository = ComponentRepository(session)
    jm: JobManagementService = JobManagementService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(client.public_key)
    await cr.create_event(client.client_id, "schedule task")

    data = await ss.read_request(request)
    payload = UpdateExecute(**data)
    artifact_id = payload.artifact_id

    try:
        content = await jm.client_local_model_start(artifact_id, client.client_id)

        return ss.create_response(content.dict())

    except ArtifactDoesNotExists as _:
        raise HTTPException(404, "Artifact does not exists")

    except TaskDoesNotExists as _:
        raise HTTPException(404, "Task does not exists")


# TODO: add endpoint for failed job executions


@client_router.post("/client/task/{artifact_id}")
async def client_post_task(
    request: Request,
    artifact_id: str,
    session: AsyncSession = Depends(get_session),
    client: Client = Depends(check_access),
):
    LOGGER.info(f"client_id={client.client_id}: complete work on artifact_id={artifact_id}")

    ss: SecurityService = SecurityService(session)
    jm: JobManagementService = JobManagementService(session)
    mr: ModelRepository = ModelRepository(session)

    model_db = await mr.create_local_model(artifact_id, client.client_id)

    await ss.setup(client.public_key)
    await ss.stream_decrypt_file(request, model_db.path)

    await jm.client_local_model_completed(artifact_id, client.client_id)

    return {}


@client_router.post("/client/metrics")
async def client_post_metrics(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Client = Depends(check_access),
):
    ss: SecurityService = SecurityService(session)
    jm: JobManagementService = JobManagementService(session)

    await ss.setup(client.public_key)

    data = await ss.read_request(request)
    metrics = Metrics(**data)

    LOGGER.info(
        f"client_id={client.client_id}: submitted new metrics for artifact_id={metrics.artifact_id} source={metrics.source}"
    )

    await jm.save_metrics(metrics)

    return {}
