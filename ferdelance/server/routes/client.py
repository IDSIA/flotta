import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy.exc import NoResultFound, SQLAlchemyError

from ferdelance.database import get_session
from ferdelance.database.schemas import Component, Model
from ferdelance.database.services import (
    ApplicationService,
    AsyncSession,
    ComponentService,
    DataSourceService,
    ModelService,
)
from ferdelance.database.tables import Application, DataSource
from ferdelance.server.exceptions import ArtifactDoesNotExists, TaskDoesNotExists
from ferdelance.server.security import check_token
from ferdelance.server.services import ActionService, JobManagementService, SecurityService
from ferdelance.shared.artifacts import Metadata, MetaDataSource, MetaFeature
from ferdelance.shared.decode import decode_from_transfer
from ferdelance.shared.models import Metrics
from ferdelance.shared.schemas import ClientJoinData, ClientJoinRequest, DownloadApp, UpdateExecute

LOGGER = logging.getLogger(__name__)


client_router = APIRouter()


@client_router.get("/client/")
async def client_home():
    return "Component 🏠"


@client_router.post("/client/join", response_class=Response)
async def client_join(
    request: Request,
    data: ClientJoinRequest,
    session: AsyncSession = Depends(get_session),
):
    """API for new client joining."""
    LOGGER.info("new client join request")

    cs: ComponentService = ComponentService(session)
    ss: SecurityService = SecurityService(session)

    if request.client is None:
        LOGGER.warning("client not set for request?")
        raise HTTPException(400)

    ip_address = request.client.host

    try:
        client_public_key = decode_from_transfer(data.public_key)

        try:
            await cs.get_by_key(client_public_key)

            raise HTTPException(403, "Invalid client data")

        except NoResultFound as e:
            LOGGER.info("joining new client")
            # create new client
            client, token = await cs.create_client(
                version=data.version,
                public_key=client_public_key,
                machine_system=data.system,
                machine_mac_address=data.mac_address,
                machine_node=data.node,
                ip_address=ip_address,
            )

            LOGGER.info(f"client_id={client.client_id}: created new client")

            await cs.create_event(client.client_id, "creation")

        LOGGER.info(f"client_id={client.client_id}: created new client")

        await ss.setup(client.public_key)

        cjd = ClientJoinData(
            id=client.client_id,
            token=token.token,
            public_key=ss.get_server_public_key(),
        )

        return ss.create_response(cjd.dict())

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception("Database error")
        raise HTTPException(500, "Internal error")

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, "Invalid client data")


@client_router.post("/client/leave")
async def client_leave(
    session: AsyncSession = Depends(get_session),
    client: Component = Depends(check_token),
):
    """API for existing client to be removed"""
    cs: ComponentService = ComponentService(session)

    LOGGER.info(f"client_id={client.client_id}: request to leave")

    await cs.client_leave(client.client_id)
    await cs.create_event(client.client_id, "left")

    return {}


@client_router.get("/client/update", response_class=Response)
async def client_update(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Component = Depends(check_token),
):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new artifact package
    - new client app package
    - nothing (keep alive)
    """

    acs: ActionService = ActionService(session)
    cs: ComponentService = ComponentService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(client.public_key)
    await cs.create_event(client.client_id, "update")

    # consume current results (if present) and compute next action
    payload: dict[str, Any] = await ss.read_request(request)

    next_action = await acs.next(client, payload)

    LOGGER.debug(f"client_id={client.client_id}: update action={next_action.action}")

    await cs.create_event(client.client_id, f"action:{next_action.action}")

    return ss.create_response(next_action.dict())


@client_router.get("/client/download/application", response_class=Response)
async def client_update_files(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Component = Depends(check_token),
):
    """
    API request by the client to get updated files. With this endpoint a client can:
    - update application software
    - obtain model files
    """
    LOGGER.info(f"client_id={client.client_id}: update files request")

    cas: ApplicationService = ApplicationService(session)
    cs: ComponentService = ComponentService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(client.public_key)
    await cs.create_event(client.client_id, "update files")

    data = await ss.read_request(request)
    payload = DownloadApp(**data)

    new_app: Application | None = await cas.get_newest_app()

    if new_app is None:
        raise HTTPException(400, "no newest version found")

    if new_app.version != payload.version:
        LOGGER.warning(
            f"client_id={client.client_id} requested app version={payload.version} while latest version={new_app.version}"
        )
        raise HTTPException(400, "Old versions are not permitted")

    await cs.update_client(client.client_id, version=payload.version)

    LOGGER.info(f"client_id={client.client_id}: requested new client version={payload.version}")

    return ss.encrypt_file(new_app.path)


@client_router.post("/client/update/metadata")
async def client_update_metadata(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Component = Depends(check_token),
):
    """Endpoint used by a client to send information regarding its metadata. These metadata includes:
    - data source available
    - summary (source, data type, min value, max value, standard deviation, ...) of features available for each data source
    """
    LOGGER.info(f"client_id={client.client_id}: update metadata request")

    cs: ComponentService = ComponentService(session)
    dss: DataSourceService = DataSourceService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(client.public_key)
    await cs.create_event(client.client_id, "update metadata")

    data = await ss.read_request(request)
    metadata = Metadata(**data)

    await dss.create_or_update_metadata(client.client_id, metadata)

    client_data_source_list: list[DataSource] = await dss.get_datasource_by_client_id(client.client_id)

    ds_list: list[MetaDataSource] = list()

    for cds in client_data_source_list:
        features = await dss.get_features_by_datasource(cds)
        ds_list.append(MetaDataSource(**cds.__dict__, features=[MetaFeature(**f.__dict__) for f in features]))

    m = Metadata(datasources=ds_list)

    return ss.create_response(m.dict())


@client_router.get("/client/task", response_class=Response)
async def client_get_task(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Component = Depends(check_token),
):
    LOGGER.info(f"client_id={client.client_id}: new task request")

    cs: ComponentService = ComponentService(session)
    jm: JobManagementService = JobManagementService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(client.public_key)
    await cs.create_event(client.client_id, "schedule task")

    data = await ss.read_request(request)
    payload = UpdateExecute(**data)
    artifact_id = payload.artifact_id

    try:
        content = await jm.client_local_model_start(artifact_id, client.client_id)

    except ArtifactDoesNotExists as _:
        raise HTTPException(404, "Artifact does not exists")

    except TaskDoesNotExists as _:
        raise HTTPException(404, "Task does not exists")

    return ss.create_response(content.dict())


# TODO: add endpoint for failed job executions


@client_router.post("/client/task/{artifact_id}")
async def client_post_task(
    request: Request,
    artifact_id: str,
    session: AsyncSession = Depends(get_session),
    client: Component = Depends(check_token),
):
    LOGGER.info(f"client_id={client.client_id}: complete work on artifact_id={artifact_id}")

    ss: SecurityService = SecurityService(session)
    jm: JobManagementService = JobManagementService(session)
    ms: ModelService = ModelService(session)

    model_session: Model = await ms.create_local_model(artifact_id, client.client_id)

    await ss.setup(client.public_key)
    await ss.stream_decrypt_file(request, model_session.path)

    await jm.client_local_model_completed(artifact_id, client.client_id)

    return {}


@client_router.post("/client/metrics")
async def client_post_metrics(
    request: Request,
    session: AsyncSession = Depends(get_session),
    client: Component = Depends(check_token),
):
    ss: SecurityService = SecurityService(session)
    jm: JobManagementService = JobManagementService(session)

    await ss.setup(client.public_key)

    data = await ss.read_request(request)
    metrics = Metrics(**data)

    LOGGER.info(
        f"client_id={client.client_id}: submitted new metrics for artifact_id={metrics.artifact_id} source={metrics.source}"
    )

    await jm.save_metrics(metrics)

    return {}
