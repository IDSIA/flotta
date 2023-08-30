from ferdelance.config import get_logger
from ferdelance.database import get_session, AsyncSession
from ferdelance.database.data import TYPE_USER
from ferdelance.schemas.workbench import (
    WorkbenchJoinRequest,
    WorkbenchProjectToken,
    WorkbenchArtifact,
)
from ferdelance.schemas.artifacts import (
    ArtifactStatus,
    Artifact,
)
from ferdelance.schemas.components import Component
from ferdelance.schemas.project import Project
from ferdelance.node.security import check_token
from ferdelance.node.services import (
    SecurityService,
    WorkbenchService,
    WorkbenchConnectService,
)
from ferdelance.shared.decode import decode_from_transfer

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, Response

from sqlalchemy.exc import SQLAlchemyError, MultipleResultsFound, NoResultFound

LOGGER = get_logger(__name__)


workbench_router = APIRouter(prefix="/workbench")


async def check_access(component: Component = Depends(check_token)) -> Component:
    try:
        if component.type_name != TYPE_USER:
            LOGGER.warning(f"client of type={component.type_name} cannot access this route")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"component_id={component.id} not found")
        raise HTTPException(403)


@workbench_router.get("/")
async def wb_home():
    return "Workbench 🔧"


@workbench_router.post("/connect", response_class=Response)
async def wb_connect(data: WorkbenchJoinRequest, session: AsyncSession = Depends(get_session)):
    LOGGER.info("new workbench connected")

    ss: SecurityService = SecurityService(session)
    wb: WorkbenchConnectService = WorkbenchConnectService(session)

    try:
        user_public_key = decode_from_transfer(data.public_key)

        wjd = await wb.connect(user_public_key)

        await ss.setup(user_public_key)
        wjd.public_key = ss.get_server_public_key()

        return ss.create_response(wjd.dict())

    except NoResultFound as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid user access")

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception("Database error")
        raise HTTPException(500, "Internal error")

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid client data")


@workbench_router.get("/project", response_class=Response)
async def wb_get_project(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={component.id}: requested a project given its token")

    ss: SecurityService = SecurityService(session)
    ws: WorkbenchService = WorkbenchService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    wpt = WorkbenchProjectToken(**data)

    try:
        project: Project = await ws.project(wpt.token)

        return ss.create_response(project.dict())

    except NoResultFound:
        LOGGER.warning(f"user_id={component.id}: request project with invalid token={wpt.token}")
        raise HTTPException(404)


@workbench_router.get("/clients", response_class=Response)
async def wb_get_client_list(
    request: Request, session: AsyncSession = Depends(get_session), component: Component = Depends(check_access)
):
    LOGGER.info(f"user_id={component.id}: requested a list of clients")

    ss: SecurityService = SecurityService(session)
    ws: WorkbenchService = WorkbenchService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    wpt = WorkbenchProjectToken(**data)

    wcl = await ws.get_client_list(wpt.token)

    return ss.create_response(wcl.dict())


@workbench_router.get("/datasources", response_class=Response)
async def wb_get_datasource_list(
    request: Request, session: AsyncSession = Depends(get_session), component: Component = Depends(check_access)
):
    LOGGER.info(f"user_id={component.id}: requested a list of available data source")

    ss: SecurityService = SecurityService(session)
    wb: WorkbenchService = WorkbenchService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    wpt = WorkbenchProjectToken(**data)

    wdsl = await wb.get_datasource_list(wpt.token)

    return ss.create_response(wdsl.dict())


@workbench_router.post("/artifact/submit", response_class=Response)
async def wb_post_artifact_submit(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={component.id}: submitted a new artifact")

    ss: SecurityService = SecurityService(session)
    wb: WorkbenchService = WorkbenchService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    artifact = Artifact(**data)

    try:
        status: ArtifactStatus = await wb.submit_artifact(artifact)

        return ss.create_response(status.dict())

    except ValueError as e:
        LOGGER.error("Artifact already exists")
        LOGGER.exception(e)
        raise HTTPException(403)


@workbench_router.get("/artifact/status", response_class=Response)
async def wb_get_artifact_status(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={component.id}: requested status of artifact")

    ss: SecurityService = SecurityService(session)
    ws: WorkbenchService = WorkbenchService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    wba = WorkbenchArtifact(**data)

    try:
        status: ArtifactStatus = await ws.get_status_artifact(wba.artifact_id)

        return ss.create_response(status.dict())
    except NoResultFound:
        LOGGER.warning(f"artifact_id={wba.artifact_id} not found in database")
        raise HTTPException(404)


@workbench_router.get("/artifact", response_class=Response)
async def wb_get_artifact(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={component.id}: requested details on artifact")

    ss: SecurityService = SecurityService(session)
    ws: WorkbenchService = WorkbenchService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    wba = WorkbenchArtifact(**data)

    try:
        artifact = await ws.get_artifact(wba.artifact_id)

        return ss.create_response(artifact.dict())

    except ValueError as e:
        LOGGER.error(f"{e}")
        raise HTTPException(404)


@workbench_router.get("/result", response_class=FileResponse)
async def wb_get_result(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    ss: SecurityService = SecurityService(session)
    ws: WorkbenchService = WorkbenchService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    wba = WorkbenchArtifact(**data)

    LOGGER.info(f"user_id={component.id}: requested result with artifact_id={wba.artifact_id}")

    try:
        result = await ws.get_result(wba.artifact_id)

        return ss.encrypt_file(result.path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except NoResultFound:
        LOGGER.warning(f"no aggregated model found for artifact_id={wba.artifact_id}")
        raise HTTPException(404)

    except MultipleResultsFound:
        # TODO: do we want to allow this?
        LOGGER.error(f"multiple aggregated models found for artifact_id={wba.artifact_id}")
        raise HTTPException(500)


@workbench_router.get(
    "/result/partial/{artifact_id}/{builder_user_id}",
    response_class=FileResponse,
)
async def wb_get_partial_result(
    artifact_id: str,
    builder_user_id: str,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(
        f"user_id={component.id}: requested partial model for artifact_id={artifact_id} from user_id={builder_user_id}"
    )

    ss: SecurityService = SecurityService(session)
    ws: WorkbenchService = WorkbenchService(session, component)

    await ss.setup(component.public_key)

    try:
        result = await ws.get_partial_result(artifact_id, builder_user_id)

        return ss.encrypt_file(result.path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except NoResultFound:
        LOGGER.warning(f"no partial model found for artifact_id={artifact_id} and user_id={builder_user_id}")
        raise HTTPException(404)

    except MultipleResultsFound:
        LOGGER.error(
            f"multiple partial models found for artifact_id={artifact_id} and user_id={builder_user_id}"
        )  # TODO: do we want to allow this?
        raise HTTPException(500)
