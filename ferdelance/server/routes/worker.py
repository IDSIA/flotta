from ferdelance.config import conf
from ferdelance.database import get_session, AsyncSession
from ferdelance.database.data import TYPE_WORKER
from ferdelance.database.repositories import ResultRepository
from ferdelance.schemas.database import Result
from ferdelance.schemas.components import Component
from ferdelance.schemas.errors import ErrorArtifact
from ferdelance.schemas.worker import WorkerTask
from ferdelance.server.security import check_token
from ferdelance.server.utils import job_manager, JobManagementService
from ferdelance.schemas.artifacts import Artifact, ArtifactStatus

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

    try:
        jms: JobManagementService = job_manager(session)

        status = await jms.submit_artifact(artifact)

        return status

    except ValueError as e:
        LOGGER.error("Artifact already exists")
        LOGGER.exception(e)
        raise HTTPException(403)


@worker_router.get("/worker/task/{job_id}", response_model=Artifact)
async def worker_get_task(
    job_id: str, session: AsyncSession = Depends(get_session), worker: Component = Depends(check_access)
):
    LOGGER.info(f"worker_id={worker.component_id}: requested job_id={job_id}")

    try:
        jms: JobManagementService = job_manager(session)

        task: WorkerTask = await jms.worker_task_start(job_id, worker.component_id)

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
    js: JobManagementService = job_manager(session)

    try:
        result_db: Result = await js.worker_result_create(job_id, worker.component_id)

        async with aiofiles.open(result_db.path, "wb") as out_file:
            while content := await file.read(conf.FILE_CHUNK_SIZE):
                await out_file.write(content)

        await js.aggregation_completed(job_id)

    except Exception as e:
        LOGGER.exception(e)
        await js.aggregation_error(job_id, f"could not save result to disk, exception: {e}")
        raise HTTPException(500)


@worker_router.post("/worker/error/")
async def post_error(
    error: ErrorArtifact,
    session: AsyncSession = Depends(get_session),
    worker: Component = Depends(check_access),
):
    artifact_id = error.artifact_id
    LOGGER.warn(f"worker_id={worker.component_id}: artifact_id={artifact_id} in error={error.message}")
    js: JobManagementService = JobManagementService(session)

    try:
        result_db = await js.worker_error(artifact_id, worker.component_id)

        await js.aggregation_error(error.artifact_id, error.message)

        async with aiofiles.open(result_db.path, "w") as f:
            content = json.dumps(error.dict())
            await f.write(content)

    except Exception as e:
        LOGGER.exception(e)
        raise HTTPException(500)


@worker_router.get("/worker/result/{result_id}", response_class=FileResponse)
async def get_result(
    result_id: str, session: AsyncSession = Depends(get_session), worker: Component = Depends(check_access)
):
    LOGGER.info(f"worker_id={worker.component_id}: request result_id={result_id}")
    try:
        rr: ResultRepository = ResultRepository(session)

        result_db: Result = await rr.get_by_id(result_id)

        if not os.path.exists(result_db.path):
            raise NoResultFound()

        return FileResponse(result_db.path)

    except NoResultFound:
        raise HTTPException(404)
