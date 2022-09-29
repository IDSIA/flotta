from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from celery.result import AsyncResult

from sqlalchemy.orm import Session
from uuid import uuid4

from ...database import get_db
from ...database.services import ArtifactService, ClientService, DataSourceService
from ...database.tables import Client, ClientDataSource, Artifact, Model
from ...worker.tasks.scheduling import schedule
from ..config import STORAGE_ARTIFACTS

from ferdelance_shared.schemas import ClientDetails, DataSource, Feature, ArtifactStatus, Artifact

import aiofiles
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


workbench_router = APIRouter()


@workbench_router.get('/workbench/')
async def wb_home():
    return 'Workbench 🔧'


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
        return HTTPException(404)

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
        created_at=ds_db.creation_time,
        features=fs,
    )


@workbench_router.post('/workbench/artifact/submit', response_model=ArtifactStatus)
async def wb_post_artifact_submit(artifact: Artifact, db: Session = Depends(get_db)):
    ars: ArtifactService = ArtifactService(db)

    artifact_id = str(uuid4())
    artifact.artifact_id = artifact_id

    path = os.path.join(STORAGE_ARTIFACTS, f'{artifact_id}.json')

    try:
        artifact_db = ars.create_artifact(artifact_id, path)

        with open(path, 'w') as f:
            json.dump(artifact.dict(), f)

        task: AsyncResult = schedule.delay(artifact.dict())

        return ArtifactStatus(
            artifact_id=artifact_id,
            status=artifact_db.status,
        )
    except ValueError as e:
        LOGGER.error('Artifact already exists')
        LOGGER.exception(e)
        return HTTPException(403)


@workbench_router.get('/workbench/artifact/{artifact_id}', response_model=ArtifactStatus)
async def wb_get_artifact_status(artifact_id: str, db: Session = Depends(get_db)):
    ars: ArtifactService = ArtifactService(db)

    artifact_db: Artifact = ars.get_artifact(artifact_id)

    # TODO: get status from celery

    if artifact_db is None:
        return HTTPException(404)

    return ArtifactStatus(
        artifact_id=artifact_id,
        status=artifact_db.status,
    )


@workbench_router.get('/workbench/download/artifact/{artifact_id}', response_model=Artifact)
async def wb_get_artifact(artifact_id: str, db: Session = Depends(get_db)):
    ars: ArtifactService = ArtifactService(db)

    artifact_db: Artifact = ars.get_artifact(artifact_id)

    if artifact_db is None:
        return HTTPException(404)

    if not os.path.exists(artifact_db.path):
        return HTTPException(404)

    async with aiofiles.open(artifact_db.path, 'r') as f:
        content = await f.read()
        data = json.loads(content)

    return Artifact(**data)


@workbench_router.get('/workbench/download/model/{artifact_id}', response_class=FileResponse)
async def wb_get_model(artifact_id: str, db: Session = Depends(get_db)):
    ars: ArtifactService = ArtifactService(db)

    artifact_db: Artifact = ars.get_artifact(artifact_id)

    if artifact_db is None:
        return HTTPException(404)

    if artifact_db.status != 'COMPLETED':
        return HTTPException(404)

    model_db: Model = ars.get_model_by_artifact(artifact_db)

    if model_db is None:
        return HTTPException(404)

    return FileResponse(model_db.path)
