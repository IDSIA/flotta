from fastapi import FastAPI, Depends

import uvicorn

from . import security
from .routes.client import client_router
from .routes.manager import manager_router
from ..database import get_db, SessionLocal, startup, Session, settings

import logging

LOGGER = logging.getLogger(__name__)


def init_api() -> FastAPI:
    api = FastAPI()

    api.include_router(client_router)
    api.include_router(manager_router)

    return api


api = init_api()


@api.on_event('startup')
async def populate_database() -> None:
    """All operations marked as `on_event('startup')` are executed when the API are started."""
    try:
        db = SessionLocal()
        startup.init_content(db)
        settings.setup_settings(db)
        security.generate_keys(db)
    finally:
        db.close()


@api.get("/")
async def root(db: Session = Depends(get_db)):
    """This is the endpoint for the home page."""
    return 'Hi! 😀'


if __name__ == '__main__':
    uvicorn.run(api, host='localhost', port='8080')
