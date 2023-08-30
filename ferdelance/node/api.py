from ferdelance.config import get_logger
from ferdelance.database import DataBase, Base
from ferdelance.node.routes import (
    client_router,
    node_router,
    task_router,
    workbench_router,
)
from ferdelance.node.startup import ServerStartup

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

LOGGER = get_logger(__name__)


def init_api() -> FastAPI:
    """Initializes the API by adding the routers."""
    api: FastAPI = FastAPI()

    LOGGER.info("Added router for /node")
    api.include_router(node_router)

    LOGGER.info("Added router for /workbench")
    api.include_router(workbench_router)

    LOGGER.info("Added router for /task")
    api.include_router(task_router)

    LOGGER.info("Added router for /client")
    api.include_router(client_router)

    return api


api = init_api()


@api.on_event("startup")
async def populate_database() -> None:
    """All operations marked as `on_event('startup')` are executed when the API are started."""
    LOGGER.info("server startup procedure started")

    try:
        inst = DataBase()

        async with inst.engine.begin() as conn:
            LOGGER.info("database creation started")
            await conn.run_sync(Base.metadata.create_all, checkfirst=True)
            LOGGER.info("database creation completed")

        async with inst.async_session() as session:
            ss = ServerStartup(session)
            await ss.startup()

    except Exception as e:
        LOGGER.exception(e)


@api.on_event("shutdown")
async def shutdown() -> None:
    LOGGER.info("server shutdown procedure started")
    inst = DataBase()
    if inst.engine:
        await inst.engine.dispose()


@api.get("/")
async def root():
    """This is the endpoint for the home page."""
    return "Hi! 😀"


@api.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    LOGGER.error(f"{request}: {exc_str}")
    content = {"status_code": 10422, "message": exc_str, "data": None}
    return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)
