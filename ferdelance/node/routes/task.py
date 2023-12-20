from ferdelance.const import TYPE_CLIENT, TYPE_NODE
from ferdelance.core.metrics import Metrics
from ferdelance.logging import get_logger
from ferdelance.node.middlewares import SignedAPIRoute, ValidSessionArgs, valid_session_args
from ferdelance.node.services import JobManagementService, TaskManagementService
from ferdelance.tasks.tasks import Task, TaskDone, TaskError, TaskRequest

from fastapi import APIRouter, Depends, HTTPException

from sqlalchemy.exc import NoResultFound


LOGGER = get_logger(__name__)


task_router = APIRouter(prefix="/task", route_class=SignedAPIRoute)


async def allow_access(args: ValidSessionArgs = Depends(valid_session_args)) -> ValidSessionArgs:
    try:
        if args.component.type_name not in (TYPE_CLIENT, TYPE_NODE):
            LOGGER.warning(f"component={args.component.id}: type={args.component.type_name} cannot access this route")
            raise HTTPException(403)

        return args
    except NoResultFound:
        LOGGER.warning(f"component={args.component.id}: not found")
        raise HTTPException(403)


@task_router.get("/")
async def get_task(
    task_request: TaskRequest,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(
        f"component={args.component.id}: request new task with artifact={task_request.artifact_id} and job={task_request.job_id}"
    )

    jms: JobManagementService = JobManagementService(
        args.session,
        args.self_component,
        args.security_service.get_private_key(),
        args.security_service.get_public_key(),
    )

    task = await jms.get_task_by_job_id(task_request.job_id)
    return task


@task_router.post("/")
async def post_task(
    task: Task,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.component.id}: new task execution")

    tms: TaskManagementService = TaskManagementService(
        args.session,
        args.component,
        args.security_service.get_private_key(),
        args.security_service.get_public_key(),
    )

    status = await tms.start_task(task, args.component.id)

    LOGGER.info(f"component={args.component.id}: executing {status}")


@task_router.post("/metrics")
async def post_metrics(
    metrics: Metrics,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(
        f"component={args.component.id}: submitted new metrics for "
        f"artifact={metrics.artifact_id} source={metrics.source}"
    )

    jms: JobManagementService = JobManagementService(
        args.session,
        args.self_component,
        args.security_service.get_private_key(),
        args.security_service.get_public_key(),
    )

    await jms.metrics(metrics)

    return


@task_router.post("/done")
async def post_done(
    done: TaskDone,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.component.id}: job={done.job_id} completed")
    jms: JobManagementService = JobManagementService(
        args.session,
        args.self_component,
        args.security_service.get_private_key(),
        args.security_service.get_public_key(),
    )
    tms: TaskManagementService = TaskManagementService(
        args.session,
        args.self_component,
        args.security_service.get_private_key(),
        args.security_service.get_public_key(),
    )

    try:
        await jms.task_completed(done.job_id)
        await jms.check(done.artifact_id)
        await tms.check(done.artifact_id)

    except Exception as e:
        LOGGER.error(
            f"component={args.self_component.id}: something went wrong with completion of job={done.job_id} "
            f"from component={args.component.id}"
        )
        LOGGER.exception(e)
        return HTTPException(500)


@task_router.post("/error")
async def post_error(
    error: TaskError,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.warning(f"component={args.component.id}: job={error.job_id} in error={error.message}")
    jms: JobManagementService = JobManagementService(
        args.session,
        args.self_component,
        args.security_service.get_private_key(),
        args.security_service.get_public_key(),
    )

    try:
        await jms.task_failed(error)

    except Exception as e:
        LOGGER.error(f"component={args.component.id}: could not save error for job={error.job_id}")
        LOGGER.exception(e)
        return HTTPException(500)
