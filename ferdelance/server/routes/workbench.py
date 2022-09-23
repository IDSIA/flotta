from http.client import HTTPException
from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse

from sqlalchemy.orm import Session
from uuid import uuid4

from ...database import get_db
from ...database.tables import Client, ClientDataSource, Artifact, Model
from ..services.ctask import ClientTaskService
from ..services.artifact import ArtifactService
from ..services.datasource import DataSourceService
from ..services.client import ClientService
from ..folders import STORAGE_ARTIFACTS

from ferdelance_shared.schemas import ClientDetails, DataSourceDetails, Feature, ArtifactStatus, Artifact

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


@workbench_router.get('/workbench/datasource/{ds_id}', response_model=DataSourceDetails)
async def wb_get_client_datasource(ds_id: int, db: Session = Depends(get_db)):
    dss: DataSourceService = DataSourceService(db)

    ds_db: ClientDataSource = dss.get_datasource_by_id(ds_id)

    if ds_db is None or ds_db.removed is True:
        raise HTTPException(404)

    f_db = dss.get_features_by_datasource(ds_db)

    fs = [Feature(**f.__dict__, created_at=f.creation_time) for f in f_db if not f.removed]

    return DataSourceDetails(
        **ds_db.__dict__,
        created_at=ds_db.creation_time,
        features=fs,
    )


@workbench_router.post('/workbench/artifact/submit', response_model=ArtifactStatus)
async def wb_post_artifact_submit(artifact: Artifact, db: Session = Depends(get_db)):
    cts: ClientTaskService = ClientTaskService(db)
    ars: ArtifactService = ArtifactService(db)
    dss: DataSourceService = DataSourceService(db)

    artifact_id = str(uuid4())

    path = os.path.join(STORAGE_ARTIFACTS, f'{artifact_id}.json')

    # TODO: check for valid data sources and features

    # TODO: start task

    try:
        artifact_db = ars.create_artifact(artifact_id, path)

        with open(path, 'w') as f:
            json.dump(artifact.dict(), f)

        # TODO this is there temporally. It will be done inside a celery worker...
        task_db = cts.create_task(artifact)

        for query in artifact.queries:
            client_id: str = dss.get_client_id_by_datasource_id(query.datasources_id)
            cts.create_client_task(artifact_db.artifact_id, client_id, task_db.task_id)

        # TODO: ...until there

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
        data = json.load(f)

    return Artifact(artifact_id=artifact_id, **data)


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
