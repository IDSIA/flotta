from ferdelance.config import conf
from ferdelance.database import get_session, AsyncSession
from ferdelance.database.data import TYPE_WORKER
from ferdelance.database.repositories import ResultRepository
from ferdelance.schemas.database import Result
from ferdelance.schemas.components import Component
from ferdelance.schemas.errors import ErrorArtifact
from ferdelance.schemas.worker import WorkerTask
from ferdelance.server.security import check_token
from ferdelance.jobs import JobManagementService
from ferdelance.schemas.artifacts import Artifact, ArtifactStatus

from ferdelance.server.services import WorkerService

from fastapi import APIRouter, Depends, HTTPException, UploadFile
from fastapi.responses import FileResponse

from sqlalchemy.exc import NoResultFound

import aiofiles
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


worker_router = APIRouter()


async def check_access(component: Component = Depends(check_token)) -> Component:
    try:
        if component.type_name != TYPE_WORKER:
            LOGGER.warning(f"client of type={component.type_name} cannot access this route")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"worker_id={component.component_id} not found")
        raise HTTPException(403)


@worker_router.post("/worker/artifact", response_model=ArtifactStatus)
async def worker_post_artifact(
    artifact: Artifact, session: AsyncSession = Depends(get_session), worker: Component = Depends(check_access)
):
    LOGGER.info(f"worker_id={worker.component_id}: sent new artifact")

    ws: WorkerService = WorkerService(session, worker)

    try:
        status = await ws.submit_artifact(artifact)

        return status

    except ValueError as e:
        LOGGER.error("Artifact already exists")
        LOGGER.exception(e)
        raise HTTPException(403)


@worker_router.get("/worker/task/{job_id}", response_model=WorkerTask)
async def worker_get_task(
    job_id: str, session: AsyncSession = Depends(get_session), worker: Component = Depends(check_access)
):
    LOGGER.info(f"worker_id={worker.component_id}: requested job_id={job_id}")

    ws: WorkerService = WorkerService(session, worker)

    try:
        task = await ws.get_task(job_id)

        return task

    except ValueError as e:
        LOGGER.error(f"{e}")
        raise HTTPException(404)


@worker_router.post("/worker/result/{job_id}")
async def post_result(
    file: UploadFile,
    job_id: str,
    session: AsyncSession = Depends(get_session),
    worker: Component = Depends(check_access),
):
    LOGGER.info(f"worker_id={worker.component_id}: send result for job_id={job_id}")

    ws: WorkerService = WorkerService(session, worker)

    try:
        result: Result = await ws.result(job_id)

        async with aiofiles.open(result.path, "wb") as out_file:
            while content := await file.read(conf.FILE_CHUNK_SIZE):
                await out_file.write(content)

        await ws.completed(job_id)

    except Exception as e:
        LOGGER.exception(e)
        await ws.error(job_id, f"could not save result to disk, exception: {e}")
        raise HTTPException(500)


@worker_router.post("/worker/error/")
async def post_error(
    error: ErrorArtifact,
    session: AsyncSession = Depends(get_session),
    worker: Component = Depends(check_access),
):
    LOGGER.warn(f"worker_id={worker.component_id}: artifact_id={error.artifact_id} in error={error.message}")

    ws: WorkerService = WorkerService(session, worker)

    try:
        result = await ws.failed(error)

        async with aiofiles.open(result.path, "w") as f:
            content = json.dumps(error.dict())
            await f.write(content)

    except Exception as e:
        LOGGER.exception(e)
        await ws.error(error.artifact_id, f"could not save result to disk, exception: {e}")
        raise HTTPException(500)


@worker_router.get("/worker/result/{result_id}", response_class=FileResponse)
async def get_result(
    result_id: str, session: AsyncSession = Depends(get_session), worker: Component = Depends(check_access)
):
    LOGGER.info(f"worker_id={worker.component_id}: request result_id={result_id}")

    ws: WorkerService = WorkerService(session, worker)

    try:
        result = await ws.get_result(result_id)

        if not os.path.exists(result.path):
            raise NoResultFound()

        return FileResponse(result.path)

    except NoResultFound:
        raise HTTPException(404)
