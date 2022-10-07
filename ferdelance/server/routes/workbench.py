from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from sqlalchemy.orm import Session

from ...database import get_db
from ...database.services import ArtifactService, ClientService, DataSourceService, ModelService
from ...database.tables import Client, ClientDataSource, Model
from ..services import JobManagementService

from ferdelance_shared.schemas import ClientDetails, DataSource, Feature, ArtifactStatus, Artifact, WorkbenchJoinData

import logging
import os

LOGGER = logging.getLogger(__name__)


workbench_router = APIRouter()


@workbench_router.get('/workbench/')
async def wb_home():
    return 'Workbench 🔧'


@workbench_router.get('/workbench/connect', response_model=WorkbenchJoinData)
async def wb_get_client_list(db: Session = Depends(get_db)):
    cs: ClientService = ClientService(db)

    token: str = cs.get_token_by_client_type('WORKBENCH')

    return WorkbenchJoinData(
        token=token,
    )


@workbench_router.get('/workbench/client/list', response_model=list[str])
async def wb_get_client_list(db: Session = Depends(get_db)):
    cs: ClientService = ClientService(db)

    clients: list[Client] = cs.get_client_list()

    return [m.client_id for m in clients if m.active is True]


@workbench_router.get('/workbench/client/{client_id}', response_model=ClientDetails)
async def wb_get_client_detail(client_id: str, db: Session = Depends(get_db)):
    cs: ClientService = ClientService(db)

    client: Client = cs.get_client_by_id(client_id)

    if client.active is False:
        raise HTTPException(404)

    return ClientDetails(
        client_id=client.client_id,
        created_at=client.creation_time,
        version=client.version
    )


@workbench_router.get('/workbench/datasource/list', response_model=list[str])
async def wb_get_datasource_list(db: Session = Depends(get_db)):
    dss: DataSourceService = DataSourceService(db)

    ds_db: list[ClientDataSource] = dss.get_datasource_list()

    LOGGER.info(f'found {len(ds_db)} datasource(s)')

    return [ds.datasource_id for ds in ds_db if ds.removed is False]


@workbench_router.get('/workbench/datasource/{ds_id}', response_model=DataSource)
async def wb_get_client_datasource(ds_id: str, db: Session = Depends(get_db)):
    dss: DataSourceService = DataSourceService(db)

    ds_db: ClientDataSource = dss.get_datasource_by_id(ds_id)

    if ds_db is None or ds_db.removed is True:
        raise HTTPException(404)

    f_db = dss.get_features_by_datasource(ds_db)

    fs = [Feature(**f.__dict__, created_at=f.creation_time) for f in f_db if not f.removed]

    return DataSource(
        **ds_db.__dict__,
        features=fs,
    )


@workbench_router.post('/workbench/artifact/submit', response_model=ArtifactStatus)
def wb_post_artifact_submit(artifact: Artifact, db: Session = Depends(get_db)):
    try:
        jms: JobManagementService = JobManagementService(db)
        return jms.submit_artifact(artifact)

    except ValueError as e:
        LOGGER.error('Artifact already exists')
        LOGGER.exception(e)
        raise HTTPException(403)


@workbench_router.get('/workbench/artifact/status/{artifact_id}', response_model=ArtifactStatus)
async def wb_get_artifact_status(artifact_id: str, db: Session = Depends(get_db)):
    ars: ArtifactService = ArtifactService(db)

    artifact_db = ars.get_artifact(artifact_id)

    # TODO: get status from celery

    if artifact_db is None:
        raise HTTPException(404)

    return ArtifactStatus(
        artifact_id=artifact_id,
        status=artifact_db.status,
    )


@workbench_router.get('/workbench/artifact/{artifact_id}', response_model=Artifact)
async def wb_get_artifact(artifact_id: str, db: Session = Depends(get_db)):
    try:
        jms: JobManagementService = JobManagementService(db)
        return jms.get_artifact(artifact_id)

    except ValueError as e:
        LOGGER.error(f'{e}')
        raise HTTPException(404)


@workbench_router.get('/workbench/model/{artifact_id}', response_class=FileResponse)
async def wb_get_model(artifact_id: str, db: Session = Depends(get_db)):
    ars: ArtifactService = ArtifactService(db)

    model_dbs: list[Model] = ars.get_models_by_artifact_id(artifact_id)

    if not model_dbs:
        raise HTTPException(404)

    model_db = [m for m in model_dbs if m.aggregated][0]

    if not os.path.exists(model_db.path):
        raise HTTPException(404)

    return FileResponse(model_db.path)
