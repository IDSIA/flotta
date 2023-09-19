from ferdelance.config import config_manager
from ferdelance.const import TYPE_CLIENT, TYPE_NODE
from ferdelance.database import get_session, AsyncSession
from ferdelance.exceptions import ArtifactDoesNotExists
from ferdelance.logging import get_logger
from ferdelance.node.middlewares import SignedAPIRoute, SessionArgs, ValidSessionArgs, valid_session_args
from ferdelance.node.services.scheduling import ScheduleActionService
from ferdelance.node.services import SecurityService, JobManagementService
from ferdelance.schemas.database import Result
from ferdelance.schemas.models import Metrics
from ferdelance.schemas.tasks import TaskParameters, TaskParametersRequest, TaskError
from ferdelance.schemas.updates import UpdateData

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse

from sqlalchemy.exc import NoResultFound


LOGGER = get_logger(__name__)


task_router = APIRouter(prefix="/task", route_class=SignedAPIRoute)


async def allow_access(args: ValidSessionArgs = Depends(valid_session_args)) -> SessionArgs:
    try:
        if args.component.type_name not in (TYPE_CLIENT, TYPE_NODE):
            LOGGER.warning(f"component={args.component.id}: type={args.component.type_name} cannot access this route")
            raise HTTPException(403)

        return args
    except NoResultFound:
        LOGGER.warning(f"component={args.component.id}: not found")
        raise HTTPException(403)


@task_router.get("/")
async def client_home():
    return "Task 🔨"


@task_router.post("/")
async def server_post_task(
    content: UpdateData,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.component.id}: new task execution")

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

    LOGGER.info(f"component={args.component.id}: executing {status}")


@task_router.get("/params", response_model=TaskParameters)
async def get_task_params(
    payload: TaskParametersRequest,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.component.id}: new task request")

    jms: JobManagementService = JobManagementService(args.session, args.component)

    try:
        content: TaskParameters = await jms.task_start(payload.job_id)
        return content

    except ArtifactDoesNotExists as e:
        LOGGER.error(f"artifact={payload.artifact_id} does not exists for job={payload.job_id}")
        LOGGER.exception(e)
        raise HTTPException(404, "Artifact does not exists")

    except ValueError as e:  # TODO: this should be TaskDoesNotExists
        LOGGER.error(f"Task does not exists for job={payload.job_id}")
        LOGGER.exception(e)
        raise HTTPException(404, "Task does not exists")


@task_router.get("/result/{result_id}", response_class=FileResponse)
async def get_result(
    result_id: str,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.component.id}: request result={result_id}")

    try:
        ws: JobManagementService = JobManagementService(args.session, args.component)
        result = await ws.get_result(result_id)

        if not result.is_aggregation and args.component.type_name == TYPE_CLIENT:
            # Only aggregation jobs can download results
            LOGGER.error(f"component={args.component.id}: Tryied to get result with result={result_id}")
            raise HTTPException(403)

        return FileResponse(result.path)

    except HTTPException as e:
        raise e

    except NoResultFound as e:
        LOGGER.error(f"component={args.component.id}: Result does not exists for result={result_id}")
        LOGGER.exception(e)
        raise HTTPException(404)

    except Exception as e:
        LOGGER.error(f"component={args.component.id}: {e}")
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

    LOGGER.info(f"component={component.id}: completed work on job={job_id}")

    ss: SecurityService = SecurityService(component.public_key)
    jms: JobManagementService = JobManagementService(session, component)

    try:
        result = await jms.task_completed(job_id)
        await ss.stream_decrypt_file(request, result.path)

        await jms.check(result.job_id)

        return {}

    except Exception as e:
        LOGGER.error(f"component={component.id}: could not save result to disk for job={job_id}")
        LOGGER.exception(e)
        raise HTTPException(500)


@task_router.post("/metrics")
async def post_metrics(
    metrics: Metrics,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(
        f"component={args.component.id}: submitted new metrics for "
        f"artifact={metrics.artifact_id} source={metrics.source}"
    )

    jms: JobManagementService = JobManagementService(args.session, args.component)

    await jms.metrics(metrics)

    return


@task_router.post("/error")
async def post_error(
    error: TaskError,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.warn(f"component={args.component.id}: job={error.job_id} in error={error.message}")
    jms: JobManagementService = JobManagementService(args.session, args.component)

    try:
        result: Result = await jms.task_failed(error)

        await jms.check(result.job_id)

        return

    except Exception as e:
        LOGGER.error(f"component={args.component.id}: could not save error to disk for job={error.job_id}")
        LOGGER.exception(e)
        return HTTPException(500)
