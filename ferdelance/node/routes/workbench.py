from ferdelance.const import TYPE_USER
from ferdelance.core.artifacts import ArtifactStatus, Artifact
from ferdelance.logging import get_logger
from ferdelance.node.middlewares import SignedAPIRoute, SessionArgs, ValidSessionArgs, session_args, valid_session_args
from ferdelance.node.services import WorkbenchService, WorkbenchConnectService
from ferdelance.node.services.tasks import TaskManagementService
from ferdelance.schemas.project import Project
from ferdelance.schemas.workbench import (
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchJoinRequest,
    WorkbenchResource,
    WorkbenchProjectToken,
    WorkbenchArtifact,
)
from ferdelance.security.checksums import str_checksum

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from sqlalchemy.exc import SQLAlchemyError, MultipleResultsFound, NoResultFound

LOGGER = get_logger(__name__)


workbench_router = APIRouter(prefix="/workbench", route_class=SignedAPIRoute)


async def allow_access(args: ValidSessionArgs = Depends(valid_session_args)) -> SessionArgs:
    try:
        if args.source.type_name != TYPE_USER:
            LOGGER.warning(f"client of type={args.source.type_name} cannot access this route")
            raise HTTPException(403)

        return args
    except NoResultFound:
        LOGGER.warning(f"component={args.source.id} not found")
        raise HTTPException(403)


@workbench_router.get("/")
async def wb_home():
    return "Workbench 🔧"


@workbench_router.post("/connect")
async def wb_connect(
    data: WorkbenchJoinRequest,
    args: SessionArgs = Depends(session_args),
):
    LOGGER.info("new workbench connected")

    wb: WorkbenchConnectService = WorkbenchConnectService(args.session)

    try:
        data_to_sign = f"{data.id}:{data.public_key}"

        args.exc.set_remote_key(data.public_key)

        args.exc.verify(data_to_sign, data.signature)
        checksum = str_checksum(data_to_sign)

        if data.checksum != checksum:
            raise ValueError("Checksum failed")

        await wb.register(data, args.ip_address)

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
    LOGGER.info(f"user={args.source.id}: requested a project given its token")

    ws: WorkbenchService = WorkbenchService(args.session, args.source, args.self_component)

    try:
        return await ws.project(wpt.token)

    except NoResultFound:
        LOGGER.warning(f"user={args.source.id}: request project with invalid token={wpt.token}")
        raise HTTPException(404)


@workbench_router.get("/clients", response_model=WorkbenchClientList)
async def wb_get_client_list(
    wpt: WorkbenchProjectToken,
    args: ValidSessionArgs = Depends(allow_access),
) -> WorkbenchClientList:
    LOGGER.info(f"user={args.source.id}: requested a list of clients")

    ws: WorkbenchService = WorkbenchService(args.session, args.source, args.self_component)

    return await ws.get_client_list(wpt.token)


@workbench_router.get("/datasources", response_model=WorkbenchDataSourceIdList)
async def wb_get_datasource_list(
    wpt: WorkbenchProjectToken,
    args: ValidSessionArgs = Depends(allow_access),
) -> WorkbenchDataSourceIdList:
    LOGGER.info(f"user={args.source.id}: requested a list of available data source")

    wb: WorkbenchService = WorkbenchService(args.session, args.source, args.self_component)

    return await wb.get_datasource_list(wpt.token)


@workbench_router.post("/artifact/submit", response_model=ArtifactStatus)
async def wb_post_artifact_submit(
    artifact: Artifact,
    args: ValidSessionArgs = Depends(allow_access),
) -> ArtifactStatus:
    LOGGER.info(f"user={args.source.id}: submitted a new artifact")

    wb: WorkbenchService = WorkbenchService(
        args.session,
        args.source,
        args.self_component,
    )
    tms: TaskManagementService = TaskManagementService(
        args.session,
        args.self_component,
        args.exc.transfer_private_key(),
        args.exc.transfer_public_key(),
    )

    try:
        status = await wb.submit_artifact(artifact)

        await tms.check(status.id)

        return status

    except ValueError as e:
        LOGGER.error("artifact already exists")
        LOGGER.exception(e)
        raise HTTPException(403)


@workbench_router.get("/artifact/status", response_model=ArtifactStatus)
async def wb_get_artifact_status(
    wba: WorkbenchArtifact,
    args: ValidSessionArgs = Depends(allow_access),
) -> ArtifactStatus:
    LOGGER.info(f"user={args.source.id}: requested status of artifact")

    ws: WorkbenchService = WorkbenchService(args.session, args.source, args.self_component)

    try:
        return await ws.get_status_artifact(wba.artifact_id)

    except NoResultFound:
        LOGGER.warning(f"artifact={wba.artifact_id} not found in database")
        raise HTTPException(404)


@workbench_router.get("/artifact", description="This endpoint returns an object of type Artifact.")
async def wb_get_artifact(
    wba: WorkbenchArtifact,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"user={args.source.id}: requested details on artifact")

    ws: WorkbenchService = WorkbenchService(args.session, args.source, args.self_component)

    try:
        artifact = await ws.get_artifact(wba.artifact_id)
        return artifact

    except ValueError as e:
        LOGGER.error(f"{e}")
        raise HTTPException(404)


# @workbench_router.post("/resource", response_model=WorkbenchResource)
# async def wb_post_resource(
#     request: Request,
#     args: ValidSessionArgs = Depends(allow_access),
# ):
#   TODO: find a way to manage resources
#     wb: WorkbenchService = WorkbenchService(args.session, args.component, args.self_component)
#     resource_id = await wb.store_resource(request.stream())
#     return WorkbenchResource(resource_id=resource_id)


@workbench_router.get("/resource/list", response_model=list[WorkbenchResource])
async def wb_get_resource_list(
    wba: WorkbenchArtifact,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"user={args.source.id}: requested resource list for artifact={wba.artifact_id}")

    ws: WorkbenchService = WorkbenchService(args.session, args.source, args.self_component)

    res_list = await ws.list_resources(wba.artifact_id)

    return [WorkbenchResource(resource_id=r.id, producer_id=r.component_id) for r in res_list]


@workbench_router.get("/resource", response_class=FileResponse)
async def wb_get_resource(
    wbr: WorkbenchResource,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"user={args.source.id}: requested resource={wbr.resource_id}")

    ws: WorkbenchService = WorkbenchService(args.session, args.source, args.self_component)

    try:
        resource = await ws.get_resource(wbr.resource_id)

        return FileResponse(resource.path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except NoResultFound:
        LOGGER.warning(f"no aggregated model found for artifact={wbr.resource_id}")
        raise HTTPException(404)

    except MultipleResultsFound:
        # TODO: do we want to allow this?
        LOGGER.error(f"multiple aggregated models found for artifact={wbr.resource_id}")
        raise HTTPException(500)

    except Exception as e:
        LOGGER.exception(e)
        raise HTTPException(500)
