from fastapi import APIRouter, Depends, HTTPException, UploadFile
from fastapi.responses import FileResponse

from ferdelance.database.tables import Model

from ...config import FILE_CHUNK_SIZE
from ...database import get_db, Session
from ...database.services import ModelService, ClientService
from ...database.tables import Model, Client
from ..services import JobManagementService
from ..security import check_token

from ferdelance_shared.schemas import Artifact, ArtifactStatus

import aiofiles
import logging
import os

LOGGER = logging.getLogger(__name__)


worker_router = APIRouter()


def check_access(db: Session = Depends(get_db), client_id: str = Depends(check_token)) -> Client:
    cs: ClientService = ClientService(db)

    client = cs.get_client_by_id(client_id)

    if client is None:
        LOGGER.warn(f'client_id={client_id} not found')
        raise HTTPException(403)

    if client.type != 'WORKER':
        LOGGER.warn(f'client of type={client.type} cannot access the route')
        raise HTTPException(403)

    return client


@worker_router.post('/worker/artifact', response_model=ArtifactStatus)
def post_artifact(artifact: Artifact, db: Session = Depends(get_db), client: Client = Depends(check_access)):
    LOGGER.info(f'client_id={client.client_id}: sent new artifact')
    try:
        jms: JobManagementService = JobManagementService(db)
        return jms.submit_artifact(artifact)

    except ValueError as e:
        LOGGER.error('Artifact already exists')
        LOGGER.exception(e)
        raise HTTPException(403)


@worker_router.get('/worker/artifact/{artifact_id}', response_model=Artifact)
async def get_artifact(artifact_id: str, db: Session = Depends(get_db), client: Client = Depends(check_access)):
    LOGGER.info(f'client_id={client.client_id}: requested artifact_id={artifact_id}')
    try:
        jms: JobManagementService = JobManagementService(db)
        return jms.get_artifact(artifact_id)

    except ValueError as e:
        LOGGER.error(f'{e}')
        raise HTTPException(404)


@worker_router.post('/worker/model/{artifact_id}')
async def post_model(file: UploadFile, artifact_id: str, db: Session = Depends(get_db), client: Client = Depends(check_access)):
    LOGGER.info(f'client_id={client.client_id}: send model for artifact_id={artifact_id}')
    try:
        ms: ModelService = ModelService(db)
        js: JobManagementService = JobManagementService(db)

        model_db = ms.create_model_aggregated(artifact_id, client.client_id)

        async with aiofiles.open(model_db.path, 'wb') as out_file:
            while content := await file.read(FILE_CHUNK_SIZE):
                await out_file.write(content)

        js.aggregation_completed(artifact_id)

    except Exception as e:
        LOGGER.exception(e)
        raise HTTPException(500)


@worker_router.get('/worker/model/{model_id}', response_class=FileResponse)
async def get_model(model_id: str, db: Session = Depends(get_db), client: Client = Depends(check_access)):
    LOGGER.info(f'client_id={client.client_id}: request model_id={model_id}')

    ms: ModelService = ModelService(db)
    model_db: Model = ms.get_model_by_id(model_id)

    if model_db is None:
        raise HTTPException(404)

    if not os.path.exists(model_db.path):
        raise HTTPException(404)

    return FileResponse(model_db.path)
