from ferdelance.const import TYPE_USER
from ferdelance.logging import get_logger
from ferdelance.node.middlewares import SignedAPIRoute, SessionArgs, ValidSessionArgs, session_args, valid_session_args
from ferdelance.node.services import WorkbenchService, WorkbenchConnectService
from ferdelance.schemas.artifacts import ArtifactStatus, Artifact
from ferdelance.schemas.project import Project
from ferdelance.schemas.workbench import (
    WorkbenchArtifactPartial,
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchJoinRequest,
    WorkbenchProjectToken,
    WorkbenchArtifact,
)
from ferdelance.shared.checksums import str_checksum

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, Response

from sqlalchemy.exc import SQLAlchemyError, MultipleResultsFound, NoResultFound

LOGGER = get_logger(__name__)


workbench_router = APIRouter(prefix="/workbench", route_class=SignedAPIRoute)


async def allow_access(args: ValidSessionArgs = Depends(valid_session_args)) -> SessionArgs:
    try:
        if args.component.type_name != TYPE_USER:
            LOGGER.warning(f"client of type={args.component.type_name} cannot access this route")
            raise HTTPException(403)

        return args
    except NoResultFound:
        LOGGER.warning(f"component={args.component.id} not found")
        raise HTTPException(403)


@workbench_router.get("/")
async def wb_home():
    return "Workbench 🔧"


@workbench_router.post("/connect", response_class=Response)
async def wb_connect(
    data: WorkbenchJoinRequest,
    args: SessionArgs = Depends(session_args),
):
    LOGGER.info("new workbench connected")

    wb: WorkbenchConnectService = WorkbenchConnectService(args.session)

    try:
        data_to_sign = f"{data.id}:{data.public_key}"

        args.security_service.set_remote_key(data.public_key)

        args.security_service.exc.verify(data_to_sign, data.signature)
        checksum = str_checksum(data_to_sign)

        if data.checksum != checksum:
            raise ValueError("Checksum failed")

        await wb.register(data, args.ip_address)

        return

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception("database error")
        raise HTTPException(500, "Internal error")

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid client data")

    except Exception as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid client data")


@workbench_router.get("/project", response_model=Project)
async def wb_get_project(
    wpt: WorkbenchProjectToken,
    args: ValidSessionArgs = Depends(allow_access),
) -> Project:
    LOGGER.info(f"user={args.component.id}: requested a project given its token")

    ws: WorkbenchService = WorkbenchService(args.session, args.component)

    try:
        return await ws.project(wpt.token)

    except NoResultFound:
        LOGGER.warning(f"user={args.component.id}: request project with invalid token={wpt.token}")
        raise HTTPException(404)


@workbench_router.get("/clients", response_model=WorkbenchClientList)
async def wb_get_client_list(
    wpt: WorkbenchProjectToken,
    args: ValidSessionArgs = Depends(allow_access),
) -> WorkbenchClientList:
    LOGGER.info(f"user={args.component.id}: requested a list of clients")

    ws: WorkbenchService = WorkbenchService(args.session, args.component)

    return await ws.get_client_list(wpt.token)


@workbench_router.get("/datasources", response_model=WorkbenchDataSourceIdList)
async def wb_get_datasource_list(
    wpt: WorkbenchProjectToken,
    args: ValidSessionArgs = Depends(allow_access),
) -> WorkbenchDataSourceIdList:
    LOGGER.info(f"user={args.component.id}: requested a list of available data source")

    wb: WorkbenchService = WorkbenchService(args.session, args.component)

    return await wb.get_datasource_list(wpt.token)


@workbench_router.post("/artifact/submit", response_model=ArtifactStatus)
async def wb_post_artifact_submit(
    artifact: Artifact,
    args: ValidSessionArgs = Depends(allow_access),
) -> ArtifactStatus:
    LOGGER.info(f"user={args.component.id}: submitted a new artifact")

    wb: WorkbenchService = WorkbenchService(args.session, args.component)

    try:
        return await wb.submit_artifact(artifact)

    except ValueError as e:
        LOGGER.error("artifact already exists")
        LOGGER.exception(e)
        raise HTTPException(403)


@workbench_router.get("/artifact/status", response_model=ArtifactStatus)
async def wb_get_artifact_status(
    wba: WorkbenchArtifact,
    args: ValidSessionArgs = Depends(allow_access),
) -> ArtifactStatus:
    LOGGER.info(f"user={args.component.id}: requested status of artifact")

    ws: WorkbenchService = WorkbenchService(args.session, args.component)

    try:
        return await ws.get_status_artifact(wba.artifact_id)

    except NoResultFound:
        LOGGER.warning(f"artifact={wba.artifact_id} not found in database")
        raise HTTPException(404)


@workbench_router.get("/artifact", response_model=Artifact)
async def wb_get_artifact(
    wba: WorkbenchArtifact,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"user={args.component.id}: requested details on artifact")

    ws: WorkbenchService = WorkbenchService(args.session, args.component)

    try:
        return await ws.get_artifact(wba.artifact_id)

    except ValueError as e:
        LOGGER.error(f"{e}")
        raise HTTPException(404)


@workbench_router.get("/result", response_class=FileResponse)
async def wb_get_result(
    wba: WorkbenchArtifact,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"user={args.component.id}: requested result with artifact={wba.artifact_id}")

    ws: WorkbenchService = WorkbenchService(args.session, args.component)

    try:
        result = await ws.get_result(wba.artifact_id)

        return FileResponse(result.path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except NoResultFound:
        LOGGER.warning(f"no aggregated model found for artifact={wba.artifact_id}")
        raise HTTPException(404)

    except MultipleResultsFound:
        # TODO: do we want to allow this?
        LOGGER.error(f"multiple aggregated models found for artifact={wba.artifact_id}")
        raise HTTPException(500)


@workbench_router.get("/result/partial", response_class=FileResponse)
async def wb_get_partial_result(
    part: WorkbenchArtifactPartial,
    args: ValidSessionArgs = Depends(allow_access),
):
    component = args.component
    artifact_id = part.artifact_id
    producer_id = part.producer_id
    iteration = part.iteration

    LOGGER.info(
        f"user={component.id}: requested partial model for artifact={artifact_id} "
        f"from builder={producer_id} iteration={iteration}"
    )

    ws: WorkbenchService = WorkbenchService(args.session, component)

    try:
        result = await ws.get_partial_result(artifact_id, producer_id, iteration)

        return FileResponse(result.path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except NoResultFound:
        LOGGER.warning(
            f"user={component.id}: no partial model found for artifact={artifact_id} "
            f"builder={producer_id} iteration={iteration}"
        )
        raise HTTPException(404)

    except MultipleResultsFound:
        # TODO: do we want to allow this?
        LOGGER.error(
            f"user={component.id}: multiple partial models found for artifact={artifact_id} "
            f"builder={producer_id} iteration={iteration}"
        )
        raise HTTPException(500)
