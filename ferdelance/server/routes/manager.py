from fastapi import APIRouter, Depends, UploadFile, Response, HTTPException, Body
from fastapi.responses import FileResponse

from sqlalchemy.orm import Session
from uuid import uuid4

from ...database import get_db, crud
from ...database.tables import ClientApp, Artifact, Model
from ..schemas.manager import *

import aiofiles
import logging
import os

LOGGER = logging.getLogger(__name__)

STORAGE_CLIENTS = str(os.path.join('.', 'storage', 'clients'))
STORAGE_ARTIFACTS = str(os.path.join('.', 'storage', 'artifacts'))
STORAGE_MODELS = str(os.path.join('.', 'storage', 'models'))

FILE_CHUNK_SIZE: int = 4096


manager_router = APIRouter()

# TODO: add safety checks on who can upload data there


@manager_router.post('/manager/upload/client', response_model=ManagerUploadClientResponse)
async def manager_upload_client(file: UploadFile, db: Session = Depends(get_db)):
    filename = file.filename
    app_id = str(uuid4())

    LOGGER.info(f'app_id={app_id} uploading new client filename={filename}')

    os.makedirs(STORAGE_CLIENTS, exist_ok=True)

    path = os.path.join(STORAGE_CLIENTS, filename)

    async with aiofiles.open(path, 'wb') as out_file:
        while content := await file.read(FILE_CHUNK_SIZE):
            await out_file.write(content)

    client_app: ClientApp = ClientApp(
        app_id=app_id,
        path=path,
        name=filename,
    )

    db.add(client_app)
    db.commit()

    return ManagerUploadClientResponse(
        upload_id=app_id,
        filename=filename,
    )


@manager_router.post('/manager/upload/client/metadata')
async def manager_upload_client_metadata(metadata: ManagerUploadClientMetadataRequest, db: Session = Depends(get_db)):
    app_id = metadata.upload_id
    LOGGER.info(f'app_id={app_id} updating metadata')

    client_app = db.query(ClientApp).filter(ClientApp.app_id == app_id).first()

    if client_app is None:
        LOGGER.info(f'app_id={app_id} not found in database')
        return HTTPException(404)

    u = {
        'active': metadata.active
    }

    if metadata.version:
        u['version'] = metadata.version
    if metadata.name:
        u['name'] = metadata.name
    if metadata.desc:
        u['description'] = metadata.desc

    LOGGER.info(f'app_id={app_id} updating with metadata={u}')

    db.query(ClientApp).filter(ClientApp.app_id == app_id).update(u)
    db.commit()


@manager_router.post('/manager/upload/artifact')
async def manager_upload_artifact(file: UploadFile, db: Session = Depends(get_db)):
    os.makedirs(STORAGE_ARTIFACTS, exist_ok=True)

    path = os.path.join(STORAGE_ARTIFACTS, file.filename)

    async with aiofiles.open(path, 'wb') as out_file:
        while content := await file.read(FILE_CHUNK_SIZE):
            await out_file.write(content)

    artifact: Artifact = Artifact(
        version='0.0',
        path=path,
        name=file.filename,
        description='',
    )

    db.add(artifact)
    db.commit()
    artifact = db.refresh(artifact)

    return Response()


@manager_router.get('/manager/download/model')
async def manager_download_model(model: ManagerDownloadModelRequest, db: Session = Depends(get_db)):

    model: Model = crud.get_model_by_id(db, model.model_id)

    if model is None:
        raise HTTPException(status_code=404)

    return FileResponse(model.path)
