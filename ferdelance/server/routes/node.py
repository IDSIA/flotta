from ferdelance.config import config_manager, get_logger
from ferdelance.database import get_session, AsyncSession
from ferdelance.database.data import TYPE_SERVER, TYPE_CLIENT
from ferdelance.schemas.components import Component, dummy
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.node import JoinRequest, ServerPublicKey
from ferdelance.server.security import check_token
from ferdelance.server.services import SecurityService, NodeService
from ferdelance.shared.decode import decode_from_transfer

from fastapi import (
    APIRouter,
    Depends,
    Request,
    HTTPException,
)
from fastapi.responses import Response

from sqlalchemy.exc import SQLAlchemyError, NoResultFound

LOGGER = get_logger(__name__)


node_router = APIRouter(prefix="/node")


async def check_access(component: Component = Depends(check_token)) -> Component:
    try:
        if component.type_name not in (TYPE_SERVER, TYPE_CLIENT):
            LOGGER.warning(f"component type={component.type_name} cannot access this router")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"component_id={component.id} not found")
        raise HTTPException(403)


async def check_distributed(component: Component = Depends(check_access)) -> Component:
    if config_manager.get().mode != "distributed":
        LOGGER.warning("requested access to distributed endpoint while in centralized mode")
        raise HTTPException(403, "Node is not in distributed mode.")

    return component


@node_router.get("/")
async def node_home():
    return "Node 🏙"


@node_router.get("/key", response_model=ServerPublicKey)
async def node_get_public_key(session: AsyncSession = Depends(get_session)):
    ss: SecurityService = SecurityService(session)
    await ss.setup()
    pk: str = ss.get_server_public_key()

    return ServerPublicKey(public_key=pk)


@node_router.post("/join", response_class=Response)
async def node_join(
    request: Request,
    data: JoinRequest,
    session: AsyncSession = Depends(get_session),
):
    ss: SecurityService = SecurityService(session)
    ns: NodeService = NodeService(session, dummy)

    if request.client is None:
        LOGGER.warning("client not set for request?")
        raise HTTPException(400)

    ip_address = request.client.host

    try:
        client_public_key = decode_from_transfer(data.public_key)

        jd = await ns.connect(client_public_key, data, ip_address)

        await ss.setup(client_public_key)
        jd.public_key = ss.get_server_public_key()

        return ss.create_response(jd.dict())

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception("Database error")
        raise HTTPException(500, "Internal error")

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid client data")


@node_router.post("/leave")
async def node_leave(
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    """API for existing client to be removed"""
    LOGGER.info(f"client_id={component.id}: request to leave")

    ns: NodeService = NodeService(session, component)

    await ns.leave()

    return {}


@node_router.post("/metadata")
async def node_metadata(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    """Endpoint used by a client to send information regarding its metadata. These metadata includes:
    - data source available
    - summary (source, data type, min value, max value, standard deviation, ...) of features available
      for each data source
    """
    LOGGER.info(f"client_id={component.id}: update metadata request")

    ss: SecurityService = SecurityService(session)
    ns: NodeService = NodeService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    metadata = Metadata(**data)
    metadata = await ns.metadata(metadata)

    return ss.create_response(metadata.dict())


@node_router.put("/add")
async def node_update_add(
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_distributed),
):
    LOGGER.info(f"component_id={component.id}: adding new node")

    ss: SecurityService = SecurityService(session)
    ns: NodeService = NodeService(session, component)

    # TODO


@node_router.put("/remove")
async def node_update_remove(
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_distributed),
):
    LOGGER.info(f"component_id={component.id}: removing node")

    ss: SecurityService = SecurityService(session)
    ns: NodeService = NodeService(session, component)

    # TODO


@node_router.put("/metadata")
async def node_update_metadata(
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_distributed),
):
    LOGGER.info(f"component_id={component.id}: updating metadata")

    ss: SecurityService = SecurityService(session)
    ns: NodeService = NodeService(session, component)

    # TODO
