from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

import sqlalchemy.orm.exc as sqlex

from ...database import get_session, AsyncSession
from ...database.services import ArtifactService, ClientService, DataSourceService
from ...database.tables import Client, ClientDataSource, Model
from ..services import JobManagementService
from ..security import check_token


from ferdelance_shared.schemas import (
    ClientDetails,
    DataSource,
    Feature,
    ArtifactStatus,
    Artifact,
    WorkbenchJoinData,
)

import logging
import os

LOGGER = logging.getLogger(__name__)


workbench_router = APIRouter()


async def check_access(session: AsyncSession, client_id) -> Client:
    cs: ClientService = ClientService(session)

    client = await cs.get_client_by_id(client_id)

    if client is None or client.type != 'WORKBENCH':
        raise HTTPException(403)

    return client


@workbench_router.get('/workbench/')
async def wb_home():
    return 'Workbench 🔧'


@workbench_router.get('/workbench/connect', response_model=WorkbenchJoinData)
async def wb_connect(session: AsyncSession = Depends(get_session)):
    LOGGER.info('new workbench connected')

    cs: ClientService = ClientService(session)

    token = await cs.get_token_by_client_type('WORKBENCH')

    if token is None:
        raise HTTPException(404)

    return WorkbenchJoinData(
        token=token,
    )


@workbench_router.get('/workbench/client/list', response_model=list[str])
async def wb_get_client_list(session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    LOGGER.info('a workbench requested a list of clients')

    await check_access(session, client_id)

    cs: ClientService = ClientService(session)

    clients: list[Client] = await cs.get_client_list()

    return [m.client_id for m in clients if m.active is True]


@workbench_router.get('/workbench/client/{req_client_id}', response_model=ClientDetails)
async def wb_get_client_detail(req_client_id: str, session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested details on client_id={req_client_id}')

    await check_access(session, client_id)

    cs: ClientService = ClientService(session)
    client: Client | None = await cs.get_client_by_id(req_client_id)

    if client is None or client.active is False:
        LOGGER.warning(f'client_id={req_client_id} not found in database or is not active')
        raise HTTPException(404)

    return ClientDetails(
        client_id=client.client_id,
        created_at=client.creation_time,
        version=client.version
    )


@workbench_router.get('/workbench/datasource/list', response_model=list[str])
async def wb_get_datasource_list(session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    LOGGER.info('a workbench requested a list of available data source')

    await check_access(session, client_id)

    dss: DataSourceService = DataSourceService(session)

    ds_session: list[ClientDataSource] = await dss.get_datasource_list()

    LOGGER.info(f'found {len(ds_session)} datasource(s)')

    return [ds.datasource_id for ds in ds_session if ds.removed is False]


@workbench_router.get('/workbench/datasource/{ds_id}', response_model=DataSource)
async def wb_get_client_datasource(ds_id: str, session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested details on datasource_id={ds_id}')

    await check_access(session, client_id)

    dss: DataSourceService = DataSourceService(session)

    ds_session: ClientDataSource | None = await dss.get_datasource_by_id(ds_id)

    if ds_session is None or ds_session.removed is True:
        LOGGER.warning(f'datasource_id={ds_id} not found in database or has been removed')
        raise HTTPException(404)

    f_session = await dss.get_features_by_datasource(ds_session)

    fs = [Feature(**f.__dict__) for f in f_session if not f.removed]

    return DataSource(
        **ds_session.__dict__,
        features=fs,
    )


@workbench_router.get('/workbench/datasource/name/{ds_id}', response_model=list[DataSource])
async def wb_get_client_datasource_by_name(ds_id: str, session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested details on datasource_name={ds_id}')

    await check_access(session, client_id)

    dss: DataSourceService = DataSourceService(session)

    ds_sessions: list[ClientDataSource] = await dss.get_datasource_by_name(ds_id)

    if not ds_sessions:
        LOGGER.warning(f'datasource_id={ds_id} not found in database or has been removed')
        raise HTTPException(404)

    ret_ds = []

    for ds_session in ds_sessions:

        f_session = await dss.get_features_by_datasource(ds_session)

        fs = [Feature(**f.__dict__) for f in f_session if not f.removed]

        ret_ds.append(DataSource(
            **ds_session.__dict__,
            features=fs,
        ))

    return ret_ds


@workbench_router.post('/workbench/artifact/submit', response_model=ArtifactStatus)
async def wb_post_artifact_submit(artifact: Artifact, session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    await check_access(session, client_id)

    LOGGER.info(f'a workbench submitted a new artifact')

    try:
        jms: JobManagementService = JobManagementService(session)
        status = await jms.submit_artifact(artifact)

        LOGGER.info(f'submitted artifact got artifact_id={status.artifact_id}')

        return status

    except ValueError as e:
        LOGGER.error('Artifact already exists')
        LOGGER.exception(e)
        raise HTTPException(403)


@workbench_router.get('/workbench/artifact/status/{artifact_id}', response_model=ArtifactStatus)
async def wb_get_artifact_status(artifact_id: str, session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested status of artifact_id={artifact_id}')

    await check_access(session, client_id)

    ars: ArtifactService = ArtifactService(session)

    artifact_session = await ars.get_artifact(artifact_id)

    # TODO: get status from celery

    if artifact_session is None:
        LOGGER.warning(f'artifact_id={artifact_id} not found in database')
        raise HTTPException(404)

    return ArtifactStatus(
        artifact_id=artifact_id,
        status=artifact_session.status,
    )


@workbench_router.get('/workbench/artifact/{artifact_id}', response_model=Artifact)
async def wb_get_artifact(artifact_id: str, session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested details on artifact_id={artifact_id}')

    await check_access(session, client_id)

    try:
        jms: JobManagementService = JobManagementService(session)
        artifact = await jms.get_artifact(artifact_id)
        return artifact

    except ValueError as e:
        LOGGER.error(f'{e}')
        raise HTTPException(404)


@workbench_router.get('/workbench/model/{artifact_id}', response_class=FileResponse)
async def wb_get_model(artifact_id: str, session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested aggregate model for artifact_id={artifact_id}')

    await check_access(session, client_id)

    ars: ArtifactService = ArtifactService(session)

    try:
        model_session: Model = await ars.get_aggregated_model(artifact_id)

        model_path = model_session.path

        if not os.path.exists(model_path):
            raise ValueError(f'model_id={model_session.model_id} not found at path={model_path}')

        return FileResponse(model_path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except sqlex.NoResultFound as _:
        LOGGER.warning(f'no aggregated model found for artifact_id={artifact_id}')
        raise HTTPException(404)

    except sqlex.MultipleResultsFound as _:
        LOGGER.error(f'multiple aggregated models found for artifact_id={artifact_id}')  # TODO: do we want to allow this?
        raise HTTPException(500)


@workbench_router.get('/workbench/model/partial/{artifact_id}/{builder_client_id}', response_class=FileResponse)
async def wb_get_partial_model(artifact_id: str, builder_client_id: str, session: AsyncSession = Depends(get_session), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested partial model for artifact_id={artifact_id} from client_id={builder_client_id}')

    await check_access(session, client_id)

    ars: ArtifactService = ArtifactService(session)

    try:

        model_session: Model = await ars.get_partial_model(artifact_id, builder_client_id)

        model_path = model_session.path

        if not os.path.exists(model_path):
            raise ValueError(f'partial model_id={model_session.model_id} not found at path={model_path}')

        return FileResponse(model_path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except sqlex.NoResultFound as _:
        LOGGER.warning(f'no partial model found for artifact_id={artifact_id} and client_id={builder_client_id}')
        raise HTTPException(404)

    except sqlex.MultipleResultsFound as _:
        LOGGER.error(f'multiple partial models found for artifact_id={artifact_id} and client_id={builder_client_id}')  # TODO: do we want to allow this?
        raise HTTPException(500)
