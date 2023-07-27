from ferdelance.database import get_session, AsyncSession
from ferdelance.database.data import TYPE_CLIENT, TYPE_WORKER
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import Result
from ferdelance.schemas.errors import TaskError
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.tasks import TaskParameters, TaskParametersRequest
from ferdelance.server.services import SecurityService, ClientService, WorkerService
from ferdelance.server.security import check_token
from ferdelance.server.exceptions import ArtifactDoesNotExists, TaskDoesNotExists

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, Response

from sqlalchemy.exc import NoResultFound

import aiofiles
import json
import logging

LOGGER = logging.getLogger(__name__)


task_router = APIRouter(prefix="/task")


async def check_access(component: Component = Depends(check_token)) -> Component:
    try:
        if component.type_name in (TYPE_CLIENT, TYPE_WORKER):
            LOGGER.warning(f"client of type={component.type_name} cannot access this route")
            raise HTTPException(403)

        return component
    except NoResultFound:
        LOGGER.warning(f"client_id={component.id} not found")
        raise HTTPException(403)


@task_router.get("/")
async def client_home():
    return "Task 🔨"


@task_router.get("/params", response_class=Response)
async def get_task_params(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"client_id={component.id}: new task request")

    ss: SecurityService = SecurityService(session)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    payload = TaskParametersRequest(**data)

    try:
        if component.type_name == TYPE_CLIENT:
            cs: ClientService = ClientService(session, component)
            content: TaskParameters = await cs.get_task(payload.job_id)

        elif component.type_name == TYPE_WORKER:
            ws: WorkerService = WorkerService(session, component)
            content: TaskParameters = await ws.get_task(payload.job_id)

        else:
            raise TaskDoesNotExists()

        return ss.create_response(content.dict())

    except ArtifactDoesNotExists as e:
        LOGGER.error(f"artifact_id={payload.artifact_id} does not exists for job_id={payload.job_id}")
        LOGGER.exception(e)
        raise HTTPException(404, "Artifact does not exists")

    except TaskDoesNotExists as e:
        LOGGER.error(f"Task does not exists for job_id={payload.job_id}")
        LOGGER.exception(e)
        raise HTTPException(404, "Task does not exists")


@task_router.get("/result/{result_id}", response_class=FileResponse)
async def get_result(
    result_id: str, session: AsyncSession = Depends(get_session), component: Component = Depends(check_access)
):
    LOGGER.info(f"component_id={component.id}: request result_id={result_id}")

    ss: SecurityService = SecurityService(session)

    await ss.setup(component.public_key)

    try:
        ws: WorkerService = WorkerService(session, component)
        result = await ws.get_result(result_id)

        if not result.is_aggregation and component.type_name == TYPE_CLIENT:
            # Only aggregation jobs can download results
            LOGGER.error(f"component_id={component.id}: Tryied to get result with result_id={result_id}")
            raise HTTPException(403)

        return ss.encrypt_file(result.path)

    except NoResultFound | ValueError as e:
        LOGGER.error(f"Result does not exists for result_id={result_id}")
        LOGGER.exception(e)
        raise HTTPException(404)


@task_router.post("/result/{job_id}")
async def post_result(
    request: Request,
    job_id: str,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.info(f"client_id={component.id}: complete work on job_id={job_id}")

    ss: SecurityService = SecurityService(session)

    await ss.setup(component.public_key)

    try:
        if component.type_name == TYPE_CLIENT:
            cs: ClientService = ClientService(session, component)
            result = await cs.task_completed(job_id)

            await ss.stream_decrypt_file(request, result.path)

            await cs.check_and_start(result)

        elif component.type_name == TYPE_WORKER:
            ws: WorkerService = WorkerService(session, component)
            result: Result = await ws.aggregation_completed(job_id)

            await ss.stream_decrypt_file(request, result.path)

            await ws.check_next_iteration(job_id)

        return {}

    except Exception as e:
        LOGGER.error(f"component_id={component.id}: could not save result to disk for job_id={job_id}")
        LOGGER.exception(e)
        raise HTTPException(500)


@task_router.post("/metrics")
async def post_metrics(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    ss: SecurityService = SecurityService(session)
    cs: ClientService = ClientService(session, component)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    metrics = Metrics(**data)

    LOGGER.info(
        f"client_id={component.id}: submitted new metrics for artifact_id={metrics.artifact_id} source={metrics.source}"
    )

    await cs.metrics(metrics)

    return {}


@task_router.post("/error")
async def post_error(
    request: Request,
    session: AsyncSession = Depends(get_session),
    component: Component = Depends(check_access),
):
    LOGGER.warn(f"client_id={component.id}: error message")

    ss: SecurityService = SecurityService(session)

    await ss.setup(component.public_key)

    data = await ss.read_request(request)
    error = TaskError(**data)

    try:
        if component.type_name == TYPE_CLIENT:
            cs: ClientService = ClientService(session, component)

            result = await cs.task_failed(error)

            await cs.check_and_start(result)

        elif component.type_name == TYPE_WORKER:
            ws: WorkerService = WorkerService(session, component)

            result = await ws.aggregation_failed(error)

        else:
            raise ValueError("Could not save error to disk!")

        LOGGER.warn(f"component_id={component.id}: job_id={error.job_id} in error={error.message}")

        async with aiofiles.open(result.path, "w") as out_file:
            content = json.dumps(error.dict())
            await out_file.write(content)

        return {}
    except Exception as e:
        LOGGER.error(f"component_id={component.id}: could not save result to disk for job_id={error.job_id}")
        LOGGER.exception(e)
        return HTTPException(500)
