from ferdelance.config import get_logger
from ferdelance.database import get_session, AsyncSession
from ferdelance.database.data import TYPE_SERVER
from ferdelance.schemas.components import Component
from ferdelance.schemas.server import ServerJoinRequest
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


server_router = APIRouter()


async def check_access(component: Component = Depends(check_token)) -> Component:
    try:
        if component.type_name != TYPE_SERVER:
            LOGGER.warning(f"client of type={component.type_name} cannot access this route")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"client_id={component.id} not found")
        raise HTTPException(403)


@server_router.get("/server/")
async def server_home():
    return "Server 🏢"


@server_router.post("/task", response_class=Response)
async def server_post_task(
    request: Request,
    data: ServerJoinRequest,
    session: AsyncSession = Depends(get_session),
):
    LOGGER.info("new task request")


@server_router.post("/result/{result_id}", response_class=Response)
async def server_post_result(
    request: Request,
    result_id: str,
    data,
    session: AsyncSession = Depends(get_session),
):
    LOGGER.info(f"received result with id={result_id}")


@server_router.post("/metrics", response_class=Response)
async def server_post_metrics(
    request: Request,
    data,
    session: AsyncSession = Depends(get_session),
):
    LOGGER.info("received metrics")
