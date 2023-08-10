from ferdelance.config import Configuration, config_manager, get_logger
from ferdelance.database import DataBase, Base
from ferdelance.server.routes import (
    client_router,
    node_router,
    server_router,
    task_router,
    workbench_router,
)
from ferdelance.server.startup import ServerStartup

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

LOGGER = get_logger(__name__)


def init_api() -> FastAPI:
    """Initializes the API by adding the routers. If the env variable `DISTRIBUTED` is set, then the server will work
    in distributed mode and the API for client node will be disabled. Otherwise, the API for servers will be disabled
    allowing new clients to connect.
    """
    api: FastAPI = FastAPI()

    conf: Configuration = config_manager.get()

    api.include_router(node_router)
    api.include_router(workbench_router)
    api.include_router(task_router)
    LOGGER.info("Added routers for /node /workbench /task")

    if conf.mode == "distributed":
        LOGGER.info("Added router for /server")
        api.include_router(server_router)
    else:
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
