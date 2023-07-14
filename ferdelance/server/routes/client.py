from typing import Any

from ferdelance.database import get_session
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.database.repositories import AsyncSession
from ferdelance.schemas.components import Component, Application
from ferdelance.schemas.errors import ClientTaskError
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.updates import DownloadApp, UpdateExecute
from ferdelance.server.services import SecurityService, ClientService
from ferdelance.server.security import check_token
from ferdelance.server.exceptions import ArtifactDoesNotExists, TaskDoesNotExists

from fastapi import (
    APIRouter,
    Depends,
    Request,
    HTTPException,
)
from fastapi.responses import Response

from sqlalchemy.exc import NoResultFound

import aiofiles
import json
import logging

LOGGER = logging.getLogger(__name__)


client_router = APIRouter(prefix="/client")


async def check_access(component: Component = Depends(check_token)) -> Component:
    try:
        if component.type_name != TYPE_CLIENT:
            LOGGER.warning(f"client of type={component.type_name} cannot access this route")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"client_id={component.id} not found")
        raise HTTPException(403)


@client_router.get("/")
async def client_home():
    return "Client 🏠"


@client_router.get("/update", response_class=Response)
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
    LOGGER.debug(f"client_id={component.id}: update request")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component)

    await ss.setup(component.public_key)

    # consume current results (if present) and compute next action
    payload: dict[str, Any] = await ss.read_request(request)

    next_action = await cs.update(payload)

    return ss.create_response(next_action.dict())


# TODO: this can be removed
@client_router.get("/download/application", response_class=Response)
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
    LOGGER.info(f"client_id={component.id}: update files request")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    payload = DownloadApp(**data)

    try:
        new_app: Application = await cs.update_files(payload)

        return ss.encrypt_file(new_app.path)
    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(400, "Old versions are not permitted")

    except NoResultFound as e:
        LOGGER.exception(e)
        raise HTTPException(404, "no newest version found")


@client_router.get("/task", response_class=Response)
async def client_get_task(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"client_id={component.id}: new task request")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    payload = UpdateExecute(**data)

    try:
        content = await cs.get_task(payload)

        return ss.create_response(content.dict())

    except ArtifactDoesNotExists as e:
        LOGGER.exception(e)
        raise HTTPException(404, "Artifact does not exists")

    except TaskDoesNotExists as e:
        LOGGER.exception(e)
        raise HTTPException(404, "Task does not exists")


@client_router.post("/result/{job_id}")
async def client_post_result(
    request: Request,
    job_id: str,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"client_id={component.id}: complete work on job_id={job_id}")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component)

    await ss.setup(component.public_key)

    try:
        result = await cs.task_completed(job_id)

        await ss.stream_decrypt_file(request, result.path)

        await cs.check_and_start(result)

        return {}
    except Exception as e:
        LOGGER.exception(e)


@client_router.post("/error/{job_id}")
async def client_post_error(
    request: Request,
    job_id: str,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"client_id={component.id}: complete work on job_id={job_id}")

    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component)

    await ss.setup(component.public_key)

    try:
        data = await ss.read_request(request)
        error = ClientTaskError(**data)
        result = await cs.task_failed(error)

        async with aiofiles.open(result.path, "w") as out_file:
            content = json.dumps(error.dict())
            await out_file.write(content)

        await cs.check_and_start(result)

        return {}
    except Exception as e:
        LOGGER.exception(e)


@client_router.post("/metrics")
async def client_post_metrics(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    metrics = Metrics(**data)

    LOGGER.info(
        f"client_id={component.id}: submitted new metrics for artifact_id={metrics.artifact_id} source={metrics.source}"
    )

    await cs.metrics(metrics)

    return {}
