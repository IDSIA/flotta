from fastapi import APIRouter, Depends, HTTPException, UploadFile
from fastapi.responses import FileResponse

from ...config import FILE_CHUNK_SIZE
from ...database import get_db, Session
from ...database.services import ModelService
from ..services import JobManagementService

from ferdelance_shared.schemas import Artifact

import aiofiles
import logging
import os

LOGGER = logging.getLogger(__name__)


worker_router = APIRouter()


@worker_router.get('/worker/artifact/{artifact_id}', response_class=Artifact)
async def get_artifact(artifact_id: str, db: Session = Depends(get_db)):
    try:
        jms: JobManagementService = JobManagementService(db)
        return jms.get_artifact(artifact_id)
    except ValueError as e:
        LOGGER.error(f'{e}')
        return HTTPException(404)


@worker_router.get('/worker/model/{model_id}', response_class=FileResponse)
async def get_model(model_id: str, db: Session = Depends(get_db)):
    ms: ModelService = ModelService(db)
    model_db = ms.get_model_by_id(model_id)

    if model_db is None:
        return HTTPException(404)

    if not os.path.exists(model_db.path):
        return HTTPException(404)

    return FileResponse(model_db.path)


@worker_router.post('/worker/model/aggregated/{artifact_id}')
async def get_model(file: UploadFile, artifact_id: str, db: Session = Depends(get_db)):
    ms: ModelService = ModelService(db)
    model_db = ms.create_model(artifact_id, file.filename == 'aggregated_model')

    async with aiofiles.open(model_db.path, 'wb') as out_file:
        while content := await file.read(FILE_CHUNK_SIZE):
            await out_file.write(content)

    return {"Result": "OK"}
