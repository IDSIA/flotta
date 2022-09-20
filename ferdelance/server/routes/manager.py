from fastapi import APIRouter, Depends, UploadFile, Response, HTTPException
from fastapi.responses import FileResponse

from sqlalchemy.orm import Session
from uuid import uuid4

from ...database import get_db, crud
from ...database.tables import ClientApp, Artifact, Model, Client
from ..schemas.manager import *
from ..config import FILE_CHUNK_SIZE
from ..folders import STORAGE_CLIENTS, STORAGE_ARTIFACTS, STORAGE_MODELS

import aiofiles
import hashlib
import logging
import os

LOGGER = logging.getLogger(__name__)


manager_router = APIRouter()

# TODO: add safety checks on who can upload data there


@manager_router.post('/manager/upload/client', response_model=ManagerUploadClientResponse)
async def manager_upload_client(file: UploadFile, db: Session = Depends(get_db)):
    filename = file.filename
    app_id = str(uuid4())

    LOGGER.info(f'app_id={app_id} uploading new client filename={filename}')

    os.makedirs(STORAGE_CLIENTS, exist_ok=True)

    path = os.path.join(STORAGE_CLIENTS, filename)

    checksum = hashlib.sha256()

    async with aiofiles.open(path, 'wb') as out_file:
        while content := await file.read(FILE_CHUNK_SIZE):
            checksum.update(content)
            await out_file.write(content)

    client_app: ClientApp = ClientApp(
        app_id=app_id,
        path=path,
        name=filename,
        checksum=checksum.hexdigest(),
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


@manager_router.get('/manager/download/model', response_class=FileResponse)
async def manager_download_model(model: ManagerDownloadModelRequest, db: Session = Depends(get_db)):

    model: Model = crud.get_model_by_id(db, model.model_id)

    if model is None:
        raise HTTPException(status_code=404)

    return FileResponse(model.path)


@manager_router.get('/manager/client/list')
async def manager_client_list(db: Session = Depends(get_db)):

    clients: list[Client] = crud.get_client_list(db)

    return [{
        'client_id': m.client_id,
        'active': m.active,
        'ip_address': m.ip_address,
    } for m in clients]


@manager_router.get('/manager/client/remove/{client_id}')
async def manager_remove_client(client_id: str, db: Session = Depends(get_db)):
    # TODO: this endpoint need to be made secure!

    LOGGER.info(f'MANAGER: client_id={client_id}: request to leave')

    client: Client = crud.get_client_by_id(db, client_id)

    if client is None:
        return HTTPException(404)

    crud.client_leave(db, client_id)
    crud.create_client_event(db, client_id, 'left')
