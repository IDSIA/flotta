from ferdelance.config import conf
from ferdelance.database import get_session, AsyncSession
from ferdelance.database.data import TYPE_USER
from ferdelance.database.services import (
    ArtifactService,
    ComponentService,
    DataSourceService,
    ModelService,
    ProjectService,
)
from ferdelance.schemas.client import ClientDetails
from ferdelance.schemas.workbench import (
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchJoinRequest,
    WorkbenchJoinData,
)
from ferdelance.schemas.artifacts import (
    ArtifactStatus,
    Artifact,
)
from ferdelance.schemas.components import Component, Token
from ferdelance.schemas.database import ServerModel
from ferdelance.schemas.project import Project
from ferdelance.schemas.workbench import (
    WorkbenchProjectToken,
    WorkbenchArtifact,
)
from ferdelance.server.security import check_token
from ferdelance.server.services import (
    JobManagementService,
    SecurityService,
)
from ferdelance.standalone.services import JobManagementLocalService
from ferdelance.shared.decode import decode_from_transfer

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, Response

from sqlalchemy.exc import SQLAlchemyError, MultipleResultsFound, NoResultFound

import logging
import os

LOGGER = logging.getLogger(__name__)


workbench_router = APIRouter()


def job_manager(session: AsyncSession) -> JobManagementService:
    if conf.STANDALONE:
        return JobManagementLocalService(session)
    return JobManagementService(session)


async def check_access(component: Component = Depends(check_token)) -> Component:
    try:
        if component.type_name != TYPE_USER:
            LOGGER.warning(f"client of type={component.type_name} cannot access this route")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"component_id={component.component_id} not found")
        raise HTTPException(403)


@workbench_router.get("/workbench/")
async def wb_home():
    return "Workbench 🔧"


@workbench_router.post("/workbench/connect", response_class=Response)
async def wb_connect(data: WorkbenchJoinRequest, session: AsyncSession = Depends(get_session)):
    LOGGER.info("new workbench connected")

    cs: ComponentService = ComponentService(session)
    ss: SecurityService = SecurityService(session)

    try:
        user_public_key = decode_from_transfer(data.public_key)

        try:
            user = await cs.get_by_key(user_public_key)

            try:
                token: Token = await cs.get_token_by_component_id(user.component_id)

            except NoResultFound:
                raise HTTPException(403, "Invalid user access")

        except NoResultFound:
            # creating new user
            user, token = await cs.create(TYPE_USER, public_key=user_public_key)

            LOGGER.info(f"user_id={user.component_id}: created new user")

        LOGGER.info(f"user_id={user.component_id}: new workbench connected")

        await ss.setup(user.public_key)

        wjd = WorkbenchJoinData(
            id=user.component_id,
            token=token.token,
            public_key=ss.get_server_public_key(),
        )

        return ss.create_response(wjd.dict())

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception("Database error")
        raise HTTPException(500, "Internal error")

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid client data")


@workbench_router.get("/workbench/clients", response_class=Response)
async def wb_get_client_list(
    request: Request, session: AsyncSession = Depends(get_session), user: Component = Depends(check_access)
):
    LOGGER.info(f"user_id={user.component_id}: requested a list of clients")

    cs: ComponentService = ComponentService(session)
    ss: SecurityService = SecurityService(session)
    ps: ProjectService = ProjectService(session)

    await ss.setup(user.public_key)

    data = await ss.read_request(request)
    wpt = WorkbenchProjectToken(**data)

    client_ids = await ps.client_ids(wpt.token)

    clients = await cs.list_clients_by_ids(client_ids)

    client_details = [ClientDetails(**c.dict()) for c in clients]

    wcl = WorkbenchClientList(clients=client_details)

    LOGGER.info(f"found {len(wcl.clients)} datasource(s)")

    return ss.create_response(wcl.dict())


@workbench_router.get("/workbench/datasources", response_class=Response)
async def wb_get_datasource_list(
    request: Request, session: AsyncSession = Depends(get_session), user: Component = Depends(check_access)
):
    LOGGER.info(f"user_id={user.component_id}: requested a list of available data source")

    dss: DataSourceService = DataSourceService(session)
    ss: SecurityService = SecurityService(session)
    ps: ProjectService = ProjectService(session)

    await ss.setup(user.public_key)

    data = await ss.read_request(request)
    wpt = WorkbenchProjectToken(**data)

    datasource_ids = await ps.datasources_ids(wpt.token)

    datasources = [await dss.load(ds_id) for ds_id in datasource_ids]

    wdsl = WorkbenchDataSourceIdList(datasources=datasources)

    LOGGER.info(f"found {len(wdsl.datasources)} datasource(s)")

    return ss.create_response(wdsl.dict())


@workbench_router.post("/workbench/artifact/submit", response_class=Response)
async def wb_post_artifact_submit(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: submitted a new artifact")

    try:
        jms: JobManagementService = job_manager(session)
        ss: SecurityService = SecurityService(session)

        await ss.setup(user.public_key)

        data = await ss.read_request(request)
        artifact = Artifact(**data)

        status = await jms.submit_artifact(artifact)

        LOGGER.info(f"submitted artifact got artifact_id={status.artifact_id}")

        return ss.create_response(status.dict())

    except ValueError as e:
        LOGGER.error("Artifact already exists")
        LOGGER.exception(e)
        raise HTTPException(403)


@workbench_router.get("/workbench/artifact/status", response_class=Response)
async def wb_get_artifact_status(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested status of an artifact")

    ars: ArtifactService = ArtifactService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    data = await ss.read_request(request)
    artifact = WorkbenchArtifact(**data)

    try:
        status: ArtifactStatus = await ars.get_status(artifact.artifact_id)

        # TODO: get status from celery

        return ss.create_response(status.dict())
    except NoResultFound as _:
        LOGGER.warning(f"artifact_id={artifact.artifact_id} not found in database")
        raise HTTPException(404)


@workbench_router.get("/workbench/artifact", response_class=Response)
async def wb_get_artifact(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested details on artifact")

    jms: JobManagementService = job_manager(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    data = await ss.read_request(request)
    artifact = WorkbenchArtifact(**data)

    try:
        artifact = await jms.get_artifact(artifact.artifact_id)

        return ss.create_response(artifact.dict())

    except ValueError as e:
        LOGGER.error(f"{e}")
        raise HTTPException(404)


@workbench_router.get("/workbench/model", response_class=FileResponse)
async def wb_get_model(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested aggregate model for an artifact")
    ms: ModelService = ModelService(session)
    ss: SecurityService = SecurityService(session)
    await ss.setup(user.public_key)

    data = await ss.read_request(request)
    artifact = WorkbenchArtifact(**data)
    artifact_id = artifact.artifact_id

    try:
        model_db: ServerModel = await ms.get_aggregated_model(artifact_id)

        model_path = model_db.path

        if not os.path.exists(model_path):
            raise ValueError(f"model_id={model_db.model_id} not found at path={model_path}")

        return ss.encrypt_file(model_path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except NoResultFound as _:
        LOGGER.warning(f"no aggregated model found for artifact_id={artifact_id}")
        raise HTTPException(404)

    except MultipleResultsFound as _:
        LOGGER.error(
            f"multiple aggregated models found for artifact_id={artifact_id}"
        )  # TODO: do we want to allow this?
        raise HTTPException(500)


@workbench_router.get(
    "/workbench/model/partial/{artifact_id}/{builder_user_id}",
    response_class=FileResponse,
)
async def wb_get_partial_model(
    artifact_id: str,
    builder_user_id: str,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(
        f"user_id={user.component_id}: requested partial model for artifact_id={artifact_id} from user_id={builder_user_id}"
    )

    try:
        ms: ModelService = ModelService(session)
        ss: SecurityService = SecurityService(session)

        await ss.setup(user.public_key)

        model_db: ServerModel = await ms.get_partial_model(artifact_id, builder_user_id)

        model_path = model_db.path

        if not os.path.exists(model_path):
            raise ValueError(f"partial model_id={model_db.model_id} not found at path={model_path}")

        return ss.encrypt_file(model_path)

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


@workbench_router.get("/workbench/project", response_class=Response)
async def wb_get_project(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested a project given its token")

    pss: ProjectService = ProjectService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    data = await ss.read_request(request)
    wpt = WorkbenchProjectToken(**data)

    try:
        LOGGER.info(f"user_id={user.component_id}: requested a project given its token")

        project: Project = await pss.get_by_token(token=wpt.token)

        LOGGER.info(f"Loaded project with project_id={project.project_id}")

        return ss.create_response(project.dict())

    except NoResultFound as _:
        LOGGER.warning(f"user_id={user.component_id}: request project with invalid token={wpt.token}")
        raise HTTPException(404)
