from ferdelance.config import conf
from ferdelance.database import get_session, AsyncSession
from ferdelance.database.data import TYPE_USER
from ferdelance.database.services import (
    ArtifactService,
    ComponentService,
    DataSourceService,
    ModelService,
    ProjectService,
    DataSourceProjectService,
)
from ferdelance.database.schemas import Component, Model, Client, Token, DataSource as DataSourceView
from ferdelance.server.security import check_token
from ferdelance.server.services import (
    JobManagementService,
    SecurityService,
)
from ferdelance.standalone.services import JobManagementLocalService
from ferdelance.shared.artifacts import (
    DataSource,
    Feature,
    ArtifactStatus,
    Artifact,
)
from ferdelance.shared.schemas import (
    ClientDetails,
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchDataSourceList,
    WorkbenchJoinRequest,
    WorkbenchJoinData,
    WorkbenchProjectDescription,
    WorkbenchProject,
)
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


@workbench_router.get("/workbench/client/list", response_class=Response)
async def wb_get_client_list(session: AsyncSession = Depends(get_session), user: Component = Depends(check_access)):
    LOGGER.info(f"user_id={user.component_id}: requested a list of clients")

    cs: ComponentService = ComponentService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    clients = await cs.list_clients()

    wcl = WorkbenchClientList(client_ids=[c.client_id for c in clients if c.active is True])

    return ss.create_response(wcl.dict())


@workbench_router.get("/workbench/client/{req_client_id}", response_class=Response)
async def wb_get_user_detail(
    req_client_id: str,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested details on client_id={req_client_id}")

    cs: ComponentService = ComponentService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    client: Client = await cs.get_client_by_id(req_client_id)

    if client.active is False:
        LOGGER.warning(f"client_id={req_client_id} not found in database or is not active")
        raise HTTPException(404)

    cd = ClientDetails(client_id=client.client_id, version=client.version)

    return ss.create_response(cd.dict())


@workbench_router.get("/workbench/datasource/list", response_class=Response)
async def wb_get_datasource_list(session: AsyncSession = Depends(get_session), user: Component = Depends(check_access)):
    LOGGER.info(f"user_id={user.component_id}: requested a list of available data source")

    dss: DataSourceService = DataSourceService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    ds_session: list[DataSourceView] = await dss.get_datasource_list()

    LOGGER.info(f"found {len(ds_session)} datasource(s)")

    wdsl = WorkbenchDataSourceIdList(datasource_ids=[ds.datasource_id for ds in ds_session if ds.removed is False])

    return ss.create_response(wdsl.dict())


@workbench_router.get("/workbench/datasource/{ds_id}", response_class=Response)
async def wb_get_client_datasource(
    ds_id: str,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_token),
):
    LOGGER.info(f"user_id={user.component_id}: requested details on datasource_id={ds_id}")

    dss: DataSourceService = DataSourceService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)
    try:
        ds_session: DataSourceView = await dss.get_datasource_by_id(ds_id)

        f_session = await dss.get_features_by_datasource(ds_session)
        tokens = await dss.get_tokens_by_datasource(ds_session)

        fs = [Feature(**f.__dict__) for f in f_session if not f.removed]

        ds = DataSource(
            **ds_session.__dict__,
            features=fs,
            tokens=tokens,
        )

        return ss.create_response(ds.dict())

    except NoResultFound:
        LOGGER.warning(f"datasource_id={ds_id} not found in database or has been removed")
        raise HTTPException(404)


@workbench_router.get("/workbench/datasource/name/{ds_name}", response_class=Response)
async def wb_get_client_datasource_by_name(
    ds_name: str,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested details on datasource_name={ds_name}")

    dss: DataSourceService = DataSourceService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    ds_dbs: list[DataSourceView] = await dss.get_datasource_by_name(ds_name)

    if not ds_dbs:
        LOGGER.warning(f"datasource_id={ds_name} not found in database or has been removed")
        raise HTTPException(404)

    ret_ds: list[DataSource] = []

    for ds_db in ds_dbs:

        f_db = await dss.get_features_by_datasource(ds_db)

        fs = [Feature(**f.__dict__) for f in f_db if not f.removed]

        ret_ds.append(
            DataSource(
                **ds_db.__dict__,
                features=fs,
            )
        )

    wdsl = WorkbenchDataSourceList(datasources=ret_ds)

    return ss.create_response(wdsl.dict())


@workbench_router.post("/workbench/artifact/submit", response_class=Response)
async def wb_post_artifact_submit(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}:  submitted a new artifact")

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


@workbench_router.get("/workbench/artifact/status/{artifact_id}", response_class=Response)
async def wb_get_artifact_status(
    artifact_id: str,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}:  requested status of artifact_id={artifact_id}")

    ars: ArtifactService = ArtifactService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    artifact_session = await ars.get_artifact(artifact_id)

    # TODO: get status from celery

    if artifact_session is None:
        LOGGER.warning(f"artifact_id={artifact_id} not found in database")
        raise HTTPException(404)

    status = ArtifactStatus(
        artifact_id=artifact_id,
        status=artifact_session.status,
    )

    return ss.create_response(status.dict())


@workbench_router.get("/workbench/artifact/{artifact_id}", response_class=Response)
async def wb_get_artifact(
    artifact_id: str,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested details on artifact_id={artifact_id}")

    try:
        jms: JobManagementService = job_manager(session)
        ss: SecurityService = SecurityService(session)

        await ss.setup(user.public_key)

        artifact = await jms.get_artifact(artifact_id)

        return ss.create_response(artifact.dict())

    except ValueError as e:
        LOGGER.error(f"{e}")
        raise HTTPException(404)


@workbench_router.get("/workbench/model/{artifact_id}", response_class=FileResponse)
async def wb_get_model(
    artifact_id: str,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested aggregate model for artifact_id={artifact_id}")

    try:
        ms: ModelService = ModelService(session)
        ss: SecurityService = SecurityService(session)

        await ss.setup(user.public_key)

        model_session: Model = await ms.get_aggregated_model(artifact_id)

        model_path = model_session.path

        if not os.path.exists(model_path):
            raise ValueError(f"model_id={model_session.model_id} not found at path={model_path}")

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

        model_session: Model = await ms.get_partial_model(artifact_id, builder_user_id)

        model_path = model_session.path

        if not os.path.exists(model_path):
            raise ValueError(f"partial model_id={model_session.model_id} not found at path={model_path}")

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


@workbench_router.get("/workbench/projects/list", response_class=Response)
async def wb_get_projects_list(
    request: Request, session: AsyncSession = Depends(get_session), user: Component = Depends(check_access)
):
    LOGGER.info(f"user_id={user.component_id}: requested a list of available projects given its tokens")

    pss: ProjectService = ProjectService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    data = await ss.read_request(request)
    token_list = data["project_tokens"]

    invalid_tokens: list[str] = []
    projects: list[WorkbenchProject] = []
    for token in token_list:
        try:
            project = await pss.get_by_token(token=token)
            projects.append(WorkbenchProject(**project.dict()))
        except NoResultFound as _:
            invalid_tokens.append(token)

    LOGGER.info(f"found {len(projects)} project(s). Invalid tokens: {invalid_tokens}")

    wbpl = WorkbenchProjectList(projects=projects)

    return ss.create_response(wbpl.dict())


@workbench_router.get("/workbench/projects", response_class=Response)
async def wb_get_project(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested a list of available projects given its tokens")

    pss: ProjectService = ProjectService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    data = await ss.read_request(request)
    project_token: str = data["project_token"]

    try:
        project = await pss.get_by_token(token=project_token)
    except NoResultFound as _:
        LOGGER.warning(f"invalid name + token combination for project token {project_token}")
        raise HTTPException(404)

    LOGGER.info(f"Loaded project {project}")

    wbpl = WorkbenchProject(**{k: str(v) for k, v in project.dict().items()})

    return ss.create_response(wbpl.dict())


@workbench_router.get("/workbench/projects/descr", response_class=Response)
async def wb_get_project_descr(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Component = Depends(check_access),
):
    LOGGER.info(f"user_id={user.component_id}: requested a list of available projects given its tokens")

    pss: ProjectService = ProjectService(session)
    dsps: DataSourceProjectService = DataSourceProjectService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    data = await ss.read_request(request)
    project_token: str = data["project_token"]

    try:
        datasources = await dsps.get_datasources_by_project(project_token == project_token)
    except NoResultFound as _:
        LOGGER.warning(f"invalid name + token combination for project name {project_name} and token {project_token}")
        raise HTTPException(404)

    wbpd: WorkbenchProjectDescription = WorkbenchProjectDescription(
        **{
            **project.dict(),
            "n_datasources": len(datasources),
            "avg_n_features": sum([ds.n_features for ds in datasources]) / len(datasources),
        }
    )

    LOGGER.info(f"Loaded project description for: {project}")

    return ss.create_response(wbpd.dict())
