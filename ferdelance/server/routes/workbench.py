from http.client import HTTPException
from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse

from sqlalchemy.orm import Session
from uuid import uuid4

from ...database import get_db, crud
from ...database.tables import Client, ClientDataSource, Artifact, Model
from ..schemas.workbench import *
from ..folders import STORAGE_ARTIFACTS

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
    clients: list[Client] = crud.get_client_list(db)

    return [m.client_id for m in clients if m.active is True]


@workbench_router.get('/workbench/client/{client_id}', response_model=ClientDetails)
async def wb_get_client_detail(client_id: str, db: Session = Depends(get_db)):
    client: Client = crud.get_client_by_id(db, client_id)

    if client.active is False:
        return HTTPException(404)

    return ClientDetails(
        client_id=client.client_id,
        created_at=client.creation_time,
        version=client.version
    )


@workbench_router.get('/workbench/datasource/list', response_model=list[int])
async def wb_get_datasource_list(db: Session = Depends(get_db)):
    ds_db: list[ClientDataSource] = crud.get_datasource_list(db)

    LOGGER.info(f'found {len(ds_db)} datasource(s)')

    return [ds.datasource_id for ds in ds_db if ds.removed is False]


@workbench_router.get('/workbench/datasource/{ds_id}', response_model=DataSourceDetails)
async def wb_get_client_datasource(ds_id: int, db: Session = Depends(get_db)):
    ds_db, f_db = crud.get_datasource_by_id(db, ds_id)

    if ds_db is None or ds_db.removed is True:
        raise HTTPException(404)

    ds = DataSource(**ds_db.__dict__, created_at=ds_db.creation_time)

    fs = [Feature(**f.__dict__, created_at=f.creation_time) for f in f_db if not f.removed]

    return DataSourceDetails(
        datasource=ds,
        features=fs,
    )


@workbench_router.post('/workbench/artifact/submit', response_model=ArtifactResponse)
async def wb_post_artifact_submit(artifact: ArtifactSubmitRequest, db: Session = Depends(get_db)):
    artifact_id = str(uuid4())

    path = os.path.join(STORAGE_ARTIFACTS, f'{artifact_id}.json')

    # TODO: check for valid data sources and features

    # TODO: start task

    try:
        artifact_db = crud.create_artifact(db, artifact_id, path)
        data = artifact.dict()

        with open(path, 'w') as f:
            json.dump(data, f)

        return ArtifactResponse(
            **data,
            artifact_id=artifact_id,
            status=artifact_db.status,
        )
    except ValueError as e:
        LOGGER.error('Artifact already exists')
        LOGGER.exception(e)
        return HTTPException(403)


@workbench_router.get('/workbench/artifact/{artifact_id}', response_model=ArtifactStatus)
async def wb_get_artifact_status(artifact_id: str, db: Session = Depends(get_db)):
    artifact_db: Artifact = crud.get_artifact(db, artifact_id)

    if artifact_db is None:
        return HTTPException(404)

    return ArtifactStatus(
        artifact_id=artifact_id,
        status=artifact_db.status,
    )


@workbench_router.get('/workbench/download/artifact/{artifact_id}', response_model=ArtifactResponse)
async def wb_get_artifact(artifact_id: str, db: Session = Depends(get_db)):
    artifact_db: Artifact = crud.get_artifact(db, artifact_id)

    if artifact_db is None:
        return HTTPException(404)

    if not os.path.exists(artifact_db.path):
        return HTTPException(404)

    with open(artifact_db.path, 'r') as f:
        data = json.load(f)

    return ArtifactResponse(artifact_id=artifact_id, status=artifact_db.status, **data)


@workbench_router.get('/workbench/download/model/{artifact_id}', response_class=FileResponse)
async def wb_get_model(artifact_id: str, db: Session = Depends(get_db)):
    artifact_db: Artifact = crud.get_artifact(db, artifact_id)

    if artifact_db is None:
        return HTTPException(404)

    if artifact_db.status != 'COMPLETED':
        return HTTPException(404)

    model_db: Model = crud.get_model_by_artifact(db, artifact_db)

    if model_db is None:
        return HTTPException(404)

    return FileResponse(model_db.path)
