from ferdelance.database import get_session
from ferdelance.database.data import TYPE_SERVER
from ferdelance.schemas.components import Component
from ferdelance.server.security import check_token

from fastapi import (
    APIRouter,
    Depends,
    Request,
    HTTPException,
)
from fastapi.responses import Response

from sqlalchemy.exc import SQLAlchemyError, NoResultFound

import logging

LOGGER = logging.getLogger(__name__)


server_router = APIRouter()


async def check_access(component: Component = Depends(check_token)) -> Component:
    try:
        if component.type_name != TYPE_SERVER:
            LOGGER.warning(f"client of type={component.type_name} cannot access this route")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"client_id={component.component_id} not found")
        raise HTTPException(403)


@server_router.get("/server/")
async def client_home():
    return "Server 🏢"
