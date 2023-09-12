from ferdelance.config import config_manager
from ferdelance.const import TYPE_CLIENT, TYPE_NODE
from ferdelance.database import get_session, AsyncSession
from ferdelance.exceptions import ArtifactDoesNotExists, TaskDoesNotExists
from ferdelance.logging import get_logger
from ferdelance.node.middlewares import SignedAPIRoute, SessionArgs, ValidSessionArgs, valid_session_args
from ferdelance.node.services.scheduling import ScheduleActionService
from ferdelance.node.services import SecurityService, ComponentService, TaskService
from ferdelance.schemas.database import Result
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.tasks import TaskParameters, TaskParametersRequest, TaskError
from ferdelance.schemas.updates import UpdateData

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse

from sqlalchemy.exc import NoResultFound

import aiofiles
import json

LOGGER = get_logger(__name__)


task_router = APIRouter(prefix="/task", route_class=SignedAPIRoute)


async def allow_access(args: ValidSessionArgs = Depends(valid_session_args)) -> SessionArgs:
    try:
        if args.component.type_name not in (TYPE_CLIENT, TYPE_NODE):
            LOGGER.warning(
                f"component_id={args.component.id}: type={args.component.type_name} cannot access this route"
            )
            raise HTTPException(403)

        return args
    except NoResultFound:
        LOGGER.warning(f"component_id={args.component.id}: not found")
        raise HTTPException(403)


@task_router.get("/")
async def client_home():
    return "Task 🔨"


@task_router.post("/")
async def server_post_task(
    content: UpdateData,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component_id={args.component.id}: new task execution")

    config = config_manager.get()

    scheduler = ScheduleActionService(
        args.self_component.id,
        args.security_service.exc.transfer_private_key(),
        config.workdir,
    )

    status = scheduler.schedule(
        args.component.url,
        args.component.public_key,
        content,
        config.datasources,
    )

    LOGGER.info(f"component_id={args.component.id}: executing {status}")


@task_router.get("/params", response_model=TaskParameters)
async def get_task_params(
    payload: TaskParametersRequest,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component_id={args.component.id}: new task request")

    try:
        if args.component.type_name == TYPE_CLIENT:
            cs: ComponentService = ComponentService(args.session, args.component)
            content: TaskParameters = await cs.get_task(payload.job_id)

        elif args.component.type_name == TYPE_NODE:
            ws: TaskService = TaskService(args.session, args.component)
            content: TaskParameters = await ws.get_task(payload.job_id)

        else:
            raise TaskDoesNotExists()

        return content

    except ArtifactDoesNotExists as e:
        LOGGER.error(f"artifact_id={payload.artifact_id} does not exists for job_id={payload.job_id}")
        LOGGER.exception(e)
        raise HTTPException(404, "Artifact does not exists")

    except ValueError as e:  # TODO: this should be TaskDoesNotExists
        LOGGER.error(f"Task does not exists for job_id={payload.job_id}")
        LOGGER.exception(e)
        raise HTTPException(404, "Task does not exists")


@task_router.get("/result/{result_id}", response_class=FileResponse)
async def get_result(
    result_id: str,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component_id={args.component.id}: request result_id={result_id}")

    try:
        ws: TaskService = TaskService(args.session, args.component)
        result = await ws.get_result(result_id)

        if not result.is_aggregation and args.component.type_name == TYPE_CLIENT:
            # Only aggregation jobs can download results
            LOGGER.error(f"component_id={args.component.id}: Tryied to get result with result_id={result_id}")
            raise HTTPException(403)

        return FileResponse(result.path)

    except HTTPException as e:
        raise e

    except NoResultFound as e:
        LOGGER.error(f"component_id={args.component.id}: Result does not exists for result_id={result_id}")
        LOGGER.exception(e)
        raise HTTPException(404)

    except Exception as e:
        LOGGER.error(f"component_id={args.component.id}: {e}")
        LOGGER.exception(e)
        raise HTTPException(500)


@task_router.post("/result/{job_id}")
async def post_result(
    request: Request,
    job_id: str,
    session: AsyncSession = Depends(get_session),
    args: SessionArgs = Depends(allow_access),
):
    component = args.self_component

    LOGGER.info(f"component_id={component.id}: complete work on job_id={job_id}")

    ss: SecurityService = SecurityService(component.public_key)

    try:
        if component.type_name == TYPE_CLIENT:
            cs: ComponentService = ComponentService(session, component)
            result = await cs.task_completed(job_id)

            await ss.stream_decrypt_file(request, result.path)

            await cs.check_and_start(result)

        elif component.type_name == TYPE_NODE:
            ws: TaskService = TaskService(session, component)
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
    metrics: Metrics,
    args: ValidSessionArgs = Depends(allow_access),
):
    cs: ComponentService = ComponentService(args.session, args.component)

    LOGGER.info(
        f"component_id={args.component.id}: submitted new metrics for "
        f"artifact_id={metrics.artifact_id} source={metrics.source}"
    )

    await cs.metrics(metrics)

    return


@task_router.post("/error")
async def post_error(
    error: TaskError,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.warn(f"component_id={args.component.id}: error message")

    try:
        if args.component.type_name == TYPE_CLIENT:
            cs: ComponentService = ComponentService(args.session, args.component)

            result = await cs.task_failed(error)

            await cs.check_and_start(result)

        elif args.component.type_name == TYPE_NODE:
            ws: TaskService = TaskService(args.session, args.component)

            result = await ws.aggregation_failed(error)

        else:
            raise ValueError("Could not save error to disk!")

        LOGGER.warn(f"component_id={args.component.id}: job_id={error.job_id} in error={error.message}")

        async with aiofiles.open(result.path, "w") as out_file:
            content = json.dumps(error.dict())
            await out_file.write(content)

        return
    except Exception as e:
        LOGGER.error(f"component_id={args.component.id}: could not save error to disk for job_id={error.job_id}")
        LOGGER.exception(e)
        return HTTPException(500)
