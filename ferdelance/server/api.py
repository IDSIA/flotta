from fastapi import FastAPI, Depends

import uvicorn

from .routes.client import client_router
from .routes.manager import manager_router
from .routes.workbench import workbench_router
from .routes.files import files_router
from .startup import ServerStartup
from ..database import get_db, SessionLocal, Session

import logging

LOGGER = logging.getLogger(__name__)


def init_api() -> FastAPI:
    api = FastAPI()

    api.include_router(client_router)
    api.include_router(manager_router)
    api.include_router(workbench_router)
    api.include_router(files_router)

    return api


api = init_api()


@api.on_event('startup')
async def populate_database() -> None:
    """All operations marked as `on_event('startup')` are executed when the API are started."""
    with SessionLocal() as db:
        ss = ServerStartup(db)
        ss.startup()


@api.get("/")
async def root(db: Session = Depends(get_db)):
    """This is the endpoint for the home page."""
    return 'Hi! 😀'


if __name__ == '__main__':
    uvicorn.run(api, host='localhost', port='1456')
