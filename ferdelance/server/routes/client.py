from ferdelance.database import get_session
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.database.repositories import AsyncSession
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.client import ClientJoinRequest
from ferdelance.schemas.updates import DownloadApp, UpdateExecute
from ferdelance.schemas.components import (
    Component,
    Application,
)
from ferdelance.schemas.models import Metrics
from ferdelance.server.services import SecurityService, ClientConnectService, ClientService
from ferdelance.server.security import check_token
from ferdelance.server.exceptions import ArtifactDoesNotExists, TaskDoesNotExists
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


async def check_access(component: Component = Depends(check_token)) -> Component:
    try:
        if component.type_name != TYPE_CLIENT:
            LOGGER.warning(f"client of type={component.type_name} cannot access this route")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"client_id={component.component_id} not found")
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

    ss: SecurityService = SecurityService(session)
    cs: ClientConnectService = ClientConnectService(session)

    if request.client is None:
        LOGGER.warning("client not set for request?")
        raise HTTPException(400)

    ip_address = request.client.host

    try:
        client_public_key = decode_from_transfer(data.public_key)

        cjd, client = await cs.connect(client_public_key, data, ip_address)

        await ss.setup(client.public_key)
        cjd.public_key = ss.get_server_public_key()

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
    component: Component = Depends(check_access),
):
    """API for existing client to be removed"""
    LOGGER.info(f"client_id={component.component_id}: request to leave")

    cs: ClientService = ClientService(session, component.component_id)

    await cs.leave()

    return {}


@client_router.get("/client/update", response_class=Response)
async def client_update(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new artifact package
    - new client app package
    - nothing (keep alive)
    """
    LOGGER.info(f"client_id={component.component_id}: update request")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component.component_id)

    await ss.setup(component.public_key)

    # consume current results (if present) and compute next action
    payload: dict[str, Any] = await ss.read_request(request)

    next_action = await cs.update(payload)

    return ss.create_response(next_action.dict())


@client_router.get("/client/download/application", response_class=Response)
async def client_update_files(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    """
    API request by the client to get updated files. With this endpoint a client can:
    - update application software
    - obtain model files
    """
    LOGGER.info(f"client_id={component.component_id}: update files request")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component.component_id)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    payload = DownloadApp(**data)

    try:
        new_app: Application = await cs.update_files(payload)

        return ss.encrypt_file(new_app.path)
    except ValueError as e:
        raise HTTPException(400, "Old versions are not permitted")

    except NoResultFound as _:
        raise HTTPException(404, "no newest version found")


@client_router.post("/client/update/metadata")
async def client_update_metadata(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    """Endpoint used by a client to send information regarding its metadata. These metadata includes:
    - data source available
    - summary (source, data type, min value, max value, standard deviation, ...) of features available for each data source
    """
    LOGGER.info(f"client_id={component.component_id}: update metadata request")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component.component_id)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    metadata = Metadata(**data)
    metadata = await cs.update_metadata(metadata)

    return ss.create_response(metadata.dict())


@client_router.get("/client/task", response_class=Response)
async def client_get_task(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"client_id={component.component_id}: new task request")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component.component_id)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    payload = UpdateExecute(**data)

    try:
        content = await cs.get_task(payload)

        return ss.create_response(content.dict())

    except ArtifactDoesNotExists as _:
        raise HTTPException(404, "Artifact does not exists")

    except TaskDoesNotExists as _:
        raise HTTPException(404, "Task does not exists")


# TODO: add endpoint for failed job executions


@client_router.post("/client/result/{job_id}")
async def client_post_result(
    request: Request,
    job_id: str,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"client_id={component.component_id}: complete work on job_id={job_id}")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component.component_id)

    await ss.setup(component.public_key)

    try:
        result = await cs.result(job_id)

        await ss.stream_decrypt_file(request, result.path)

        await cs.check(result)

        return {}
    except Exception as e:
        LOGGER.exception(e)


@client_router.post("/client/metrics")
async def client_post_metrics(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component.component_id)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    metrics = Metrics(**data)

    LOGGER.info(
        f"client_id={component.component_id}: submitted new metrics for artifact_id={metrics.artifact_id} source={metrics.source}"
    )

    await cs.metrics(metrics)

    return {}
