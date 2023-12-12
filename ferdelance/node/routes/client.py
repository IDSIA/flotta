from ferdelance.const import TYPE_CLIENT
from ferdelance.logging import get_logger
from ferdelance.node.middlewares import SignedAPIRoute, ValidSessionArgs, valid_session_args
from ferdelance.node.services import JobManagementService
from ferdelance.schemas.client import ClientUpdate
from ferdelance.schemas.updates import UpdateData

from fastapi import APIRouter, Depends, HTTPException

from sqlalchemy.exc import NoResultFound

LOGGER = get_logger(__name__)


client_router = APIRouter(prefix="/client", route_class=SignedAPIRoute)


async def allow_access(args: ValidSessionArgs = Depends(valid_session_args)) -> ValidSessionArgs:
    try:
        if args.component.type_name != TYPE_CLIENT:
            LOGGER.warning(
                f"component={args.component.id}: client of type={args.component.type_name} cannot access this route"
            )
            raise HTTPException(403)

        return args
    except NoResultFound:
        LOGGER.warning(f"component={args.component.id} not found")
        raise HTTPException(403)


@client_router.get("/")
async def client_home():
    return "Client 🏠"


@client_router.get("/update", response_model=UpdateData)
async def client_update(
    _: ClientUpdate,
    args: ValidSessionArgs = Depends(allow_access),
) -> UpdateData:
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new artifact package
    - new client app package
    - nothing (keep alive)
    """
    LOGGER.debug(f"component={args.component.id}: update request")

    jms: JobManagementService = JobManagementService(
        args.session,
        args.self_component,
        args.security_service.get_private_key(),
        args.security_service.get_public_key(),
    )

    next_action = await jms.update(args.component)

    return next_action
