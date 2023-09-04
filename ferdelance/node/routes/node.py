from ferdelance.config import config_manager
from ferdelance.const import TYPE_NODE, TYPE_CLIENT
from ferdelance.logging import get_logger
from ferdelance.node.middlewares import SessionArgs, session_args
from ferdelance.node.services import NodeService
from ferdelance.schemas.components import Component, dummy
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.node import NodeJoinRequest, ServerPublicKey
from ferdelance.shared.decode import decode_from_transfer

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response

from sqlalchemy.exc import SQLAlchemyError, NoResultFound

LOGGER = get_logger(__name__)


node_router = APIRouter(prefix="/node")


async def allow_access(args: SessionArgs = Depends(session_args)) -> SessionArgs:
    try:
        if args.component.type_name not in (TYPE_CLIENT, TYPE_NODE):
            LOGGER.warning(
                f"component_id={args.component.id}: type={args.component.type_name} cannot access this router"
            )
            raise HTTPException(403)

        return args
    except NoResultFound:
        LOGGER.warning(f"component_id={args.component.id}: not found")
        raise HTTPException(403)


async def check_distributed(component: Component = Depends(allow_access)) -> Component:
    if config_manager.get().mode != "distributed":
        LOGGER.warning("requested access to distributed endpoint while in centralized mode")
        raise HTTPException(403, "Node is not in distributed mode.")

    return component


@node_router.get("/")
async def node_home():
    return "Node 🏙"


@node_router.get("/key", response_model=ServerPublicKey)
async def node_get_public_key(
    args: SessionArgs = Depends(session_args),
):
    pk = args.security_service.get_server_public_key()

    return ServerPublicKey(public_key=pk)


@node_router.post("/join", response_class=Response)
async def node_join(
    data: NodeJoinRequest,
    args: SessionArgs = Depends(session_args),
):
    ns: NodeService = NodeService(args.session, dummy)

    try:
        data.public_key = decode_from_transfer(data.public_key)

        await args.security_service.setup(data.public_key)
        args.security_service.exc.verify(f"{data.id}:{data.checksum}", data.signature)

        if data.checksum != args.checksum:
            raise ValueError("Checksum failed")

        jd = await ns.connect(data, args.ip_address)

        return jd

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception("Database error")
        raise HTTPException(500, "Internal error")

    except ValueError | Exception as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid data")


@node_router.post("/leave")
async def node_leave(
    args: SessionArgs = Depends(allow_access),
):
    """API for existing client to be removed"""
    LOGGER.info(f"component_id={args.component.id}: request to leave")

    ns: NodeService = NodeService(args.session, args.component)

    await ns.leave()

    return {}


@node_router.post("/metadata", response_model=Metadata)
async def node_metadata(
    metadata: Metadata,
    args: SessionArgs = Depends(allow_access),
):
    """Endpoint used by a client to send information regarding its metadata. These metadata includes:
    - data source available
    - summary (source, data type, min value, max value, standard deviation, ...) of features available
      for each data source
    """
    LOGGER.info(f"component_id={args.component.id}: update metadata request")

    ns: NodeService = NodeService(args.session, args.component)

    metadata = await ns.metadata(metadata)

    return metadata


@node_router.put("/add")
async def node_update_add(
    args: SessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component_id={args.component.id}: adding new node")

    ns: NodeService = NodeService(args.session, args.component)

    # TODO


@node_router.put("/remove")
async def node_update_remove(
    args: SessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component_id={args.component.id}: removing node")

    ns: NodeService = NodeService(args.session, args.component)

    # TODO


@node_router.put("/metadata")
async def node_update_metadata(
    args: SessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component_id={args.component.id}: updating metadata")

    ns: NodeService = NodeService(args.session, args.component)

    # TODO
