from fastapi import APIRouter, Depends, HTTPException, UploadFile
from fastapi.responses import FileResponse

from ferdelance.database.tables import Model

from ...config import FILE_CHUNK_SIZE
from ...database import get_db, Session
from ...database.services import ModelService
from ...database.tables import Model
from ..services import JobManagementService
from ..security import check_token

from ferdelance_shared.schemas import Artifact, ArtifactStatus

import aiofiles
import logging
import os

LOGGER = logging.getLogger(__name__)


files_router = APIRouter()


@files_router.post('/files/artifact/', response_model=ArtifactStatus)
def post_artifact(artifact: Artifact, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    try:
        jms: JobManagementService = JobManagementService(db)
        return jms.submit_artifact(artifact)

    except ValueError as e:
        LOGGER.error('Artifact already exists')
        LOGGER.exception(e)
        return HTTPException(403)


@files_router.get('/files/artifact/{artifact_id}', response_class=Artifact)
async def get_artifact(artifact_id: str, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    try:
        jms: JobManagementService = JobManagementService(db)
        return jms.get_artifact(artifact_id)

    except ValueError as e:
        LOGGER.exception(e)
        return HTTPException(404)


@files_router.post('/files/model/{artifact_id}')
async def post_model(file: UploadFile, artifact_id: str, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    try:
        ms: ModelService = ModelService(db)
        model_db = ms.create_model(artifact_id, client_id, client_id == 'WORKER')

        async with aiofiles.open(model_db.path, 'wb') as out_file:
            while content := await file.read(FILE_CHUNK_SIZE):
                await out_file.write(content)

    except Exception as e:
        LOGGER.exception(e)
        return HTTPException(500)


@files_router.get('/files/model/{model_id}', response_class=FileResponse)
async def get_model(model_id: str, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    ms: ModelService = ModelService(db)
    model_db: Model = ms.get_model_by_id(model_id)

    if model_db is None:
        return HTTPException(404)

    if not os.path.exists(model_db.path):
        return HTTPException(404)

    return FileResponse(model_db.path)
