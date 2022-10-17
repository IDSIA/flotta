from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from sqlalchemy.orm import Session

from ...database import get_db
from ...database.services import ArtifactService, ClientService, DataSourceService
from ...database.tables import Client, ClientDataSource, Model
from ..services import JobManagementService
from ..security import check_token


from ferdelance_shared.schemas import ClientDetails, DataSource, Feature, ArtifactStatus, Artifact, WorkbenchJoinData

import logging
import os

LOGGER = logging.getLogger(__name__)


workbench_router = APIRouter()


def check_access(db: Session, client_id) -> Client:
    cs: ClientService = ClientService(db)

    client = cs.get_client_by_id(client_id)

    if client is None or client.type != 'WORKBENCH':
        raise HTTPException(403)

    return client


@workbench_router.get('/workbench/')
async def wb_home():
    return 'Workbench 🔧'


@workbench_router.get('/workbench/connect', response_model=WorkbenchJoinData)
async def wb_connect(db: Session = Depends(get_db)):
    LOGGER.info('new workbench connected')

    cs: ClientService = ClientService(db)

    token = cs.get_token_by_client_type('WORKBENCH')

    if token is None:
        raise HTTPException(404)

    return WorkbenchJoinData(
        token=token,
    )


@workbench_router.get('/workbench/client/list', response_model=list[str])
async def wb_get_client_list(db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    LOGGER.info('a workbench requested a list of clients')

    check_access(db, client_id)

    cs: ClientService = ClientService(db)

    clients: list[Client] = cs.get_client_list()

    return [m.client_id for m in clients if m.active is True]


@workbench_router.get('/workbench/client/{req_client_id}', response_model=ClientDetails)
async def wb_get_client_detail(req_client_id: str, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested details on client_id={req_client_id}')

    check_access(db, client_id)

    cs: ClientService = ClientService(db)
    client: Client = cs.get_client_by_id(req_client_id)

    if client is None or client.active is False:
        LOGGER.warn(f'client_id={req_client_id} not found in database or is not active')
        raise HTTPException(404)

    return ClientDetails(
        client_id=client.client_id,
        created_at=client.creation_time,
        version=client.version
    )


@workbench_router.get('/workbench/datasource/list', response_model=list[str])
async def wb_get_datasource_list(db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    LOGGER.info('a workbench requested a list of available data source')

    check_access(db, client_id)

    dss: DataSourceService = DataSourceService(db)

    ds_db: list[ClientDataSource] = dss.get_datasource_list()

    LOGGER.info(f'found {len(ds_db)} datasource(s)')

    return [ds.datasource_id for ds in ds_db if ds.removed is False]


@workbench_router.get('/workbench/datasource/{ds_id}', response_model=DataSource)
async def wb_get_client_datasource(ds_id: str, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested details on datasource_id={ds_id}')

    check_access(db, client_id)

    dss: DataSourceService = DataSourceService(db)

    ds_db: ClientDataSource = dss.get_datasource_by_id(ds_id)

    if ds_db is None or ds_db.removed is True:
        LOGGER.warn(f'datasource_id={ds_id} not found in database or has been removed')
        raise HTTPException(404)

    f_db = dss.get_features_by_datasource(ds_db)

    fs = [Feature(**f.__dict__) for f in f_db if not f.removed]

    return DataSource(
        **ds_db.__dict__,
        features=fs,
    )


@workbench_router.post('/workbench/artifact/submit', response_model=ArtifactStatus)
def wb_post_artifact_submit(artifact: Artifact, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    check_access(db, client_id)

    try:
        jms: JobManagementService = JobManagementService(db)
        status = jms.submit_artifact(artifact)

        LOGGER.info(f'a workbench submitted a new with assigned artifact_id={status.artifact_id}')

        return status

    except ValueError as e:
        LOGGER.error('Artifact already exists')
        LOGGER.exception(e)
        raise HTTPException(403)


@workbench_router.get('/workbench/artifact/status/{artifact_id}', response_model=ArtifactStatus)
async def wb_get_artifact_status(artifact_id: str, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested status of artifact_id={artifact_id}')

    check_access(db, client_id)

    ars: ArtifactService = ArtifactService(db)

    artifact_db = ars.get_artifact(artifact_id)

    # TODO: get status from celery

    if artifact_db is None:
        LOGGER.warn(f'artifact_id={artifact_id} not found in database')
        raise HTTPException(404)

    return ArtifactStatus(
        artifact_id=artifact_id,
        status=artifact_db.status,
    )


@workbench_router.get('/workbench/artifact/{artifact_id}', response_model=Artifact)
async def wb_get_artifact(artifact_id: str, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested details on artifact_id={artifact_id}')

    check_access(db, client_id)

    try:
        jms: JobManagementService = JobManagementService(db)
        return jms.get_artifact(artifact_id)

    except ValueError as e:
        LOGGER.error(f'{e}')
        raise HTTPException(404)


@workbench_router.get('/workbench/model/{artifact_id}', response_class=FileResponse)
async def wb_get_model(artifact_id: str, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    LOGGER.info(f'a workbench requested aggregate model for artifact_id={artifact_id}')

    check_access(db, client_id)

    ars: ArtifactService = ArtifactService(db)

    model_dbs: list[Model] = ars.get_models_by_artifact_id(artifact_id)

    if not model_dbs:
        raise HTTPException(404)

    model_db = [m for m in model_dbs if m.aggregated][0]

    model_path = model_db.path

    if not os.path.exists(model_path):
        LOGGER.warn(f'model_id={model_db.model_id} not found at path={model_path}')
        raise HTTPException(404)

    return FileResponse(model_path)
