from typing import Any

from ferdelance.config import get_logger
from ferdelance.database import get_session
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.database.repositories import AsyncSession
from ferdelance.schemas.components import Component, Application
from ferdelance.schemas.updates import DownloadApp
from ferdelance.server.services import SecurityService, ClientService
from ferdelance.server.security import check_token

from fastapi import (
    APIRouter,
    Depends,
    Request,
    HTTPException,
)
from fastapi.responses import Response

from sqlalchemy.exc import NoResultFound

LOGGER = get_logger(__name__)


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
