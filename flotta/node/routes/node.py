from flotta.const import TYPE_NODE, TYPE_CLIENT
from flotta.database.repositories.component import ComponentRepository
from flotta.logging import get_logger
from flotta.node.middlewares import SignedAPIRoute, SessionArgs, session_args, valid_session_args, ValidSessionArgs
from flotta.node.services import NodeService
from flotta.schemas.components import Component
from flotta.schemas.metadata import Metadata
from flotta.schemas.node import JoinData, NodeJoinRequest, NodeMetadata, NodePublicKey
from flotta.security.checksums import str_checksum

from fastapi import APIRouter, Depends, HTTPException, Response

from sqlalchemy.exc import SQLAlchemyError, NoResultFound

LOGGER = get_logger(__name__)


node_router = APIRouter(prefix="/node", route_class=SignedAPIRoute)


async def allow_access(args: ValidSessionArgs = Depends(valid_session_args)) -> ValidSessionArgs:
    try:
        if args.source.type_name not in (TYPE_CLIENT, TYPE_NODE):
            LOGGER.warning(f"component={args.source.id}: type={args.source.type_name} cannot access this router")
            raise HTTPException(403, "Access Denied")

        return args
    except NoResultFound:
        LOGGER.warning(f"component={args.source.id}: not found")
        raise HTTPException(403, "Access Denied")


@node_router.get("/")
async def node_home():
    return "Node 🏙"


@node_router.get("/key", response_model=NodePublicKey)
async def node_get_public_key(
    args: SessionArgs = Depends(session_args),
):
    pk = args.exc.transfer_public_key()

    return NodePublicKey(public_key=pk)


@node_router.post("/join", response_model=JoinData)
async def node_join(
    data: NodeJoinRequest,
    args: SessionArgs = Depends(session_args),
) -> JoinData:
    LOGGER.info("new component joining")

    try:
        data_to_sign = f"{data.id}:{data.public_key}"

        args.exc.set_remote_key(data.id, data.public_key)

        args.exc.verify(data_to_sign, data.signature)
        checksum = str_checksum(data_to_sign)

        if data.checksum != checksum:
            raise ValueError("Checksum failed")

        ns: NodeService = NodeService(args.session, args.self_component)
        join_data = await ns.connect(data, args.ip_address)

        return join_data

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception("database error")
        raise HTTPException(500, "Internal error")

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid data")

    except Exception as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid data")


@node_router.post("/leave", response_class=Response)
async def node_leave(
    args: ValidSessionArgs = Depends(allow_access),
) -> None:
    """API for existing client to be removed"""
    LOGGER.info(f"component={args.source.id}: request to leave")

    ns: NodeService = NodeService(args.session, args.self_component)

    await ns.leave(args.source)


@node_router.post("/metadata", response_model=Metadata)
async def node_metadata(
    metadata: Metadata,
    args: ValidSessionArgs = Depends(allow_access),
):
    """Endpoint used by a client to send information regarding its metadata. These metadata includes:
    - data source available
    - summary (source, data type, min value, max value, standard deviation, ...) of features available
      for each data source
    """
    LOGGER.info(f"component={args.source.id}: update metadata request")

    ns: NodeService = NodeService(args.session, args.self_component)

    return await ns.metadata(args.source, metadata)


@node_router.put("/add")
async def node_update_add(
    new_component: Component,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.source.id}: adding new node component={new_component.id}")

    try:
        ns: NodeService = NodeService(args.session, args.self_component)

        await ns.add(new_component)

    except Exception as e:
        LOGGER.error(f"Could not add new component with component={new_component.id} from component={args.source.id}")
        LOGGER.exception(e)
        raise HTTPException(500)


@node_router.put("/remove")
async def node_update_remove(
    component: Component,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.source.id}: removing node")
    ns: NodeService = NodeService(args.session, args.self_component)

    try:
        await ns.remove(component)

    except Exception as e:
        LOGGER.error(f"Could not remove component with component={component.id} from component={args.source.id}")
        LOGGER.exception(e)
        raise HTTPException(500)


@node_router.put("/metadata")
async def node_update_metadata(
    node_metadata: NodeMetadata,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.source.id}: updating metadata")

    cr: ComponentRepository = ComponentRepository(args.session)

    component = await cr.get_by_id(node_metadata.id)

    ns: NodeService = NodeService(args.session, args.self_component)

    await ns.metadata(component, node_metadata.metadata)
