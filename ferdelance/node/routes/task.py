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
        if args.source.type_name not in (TYPE_CLIENT, TYPE_NODE):
            LOGGER.warning(f"component={args.source.id}: type={args.source.type_name} cannot access this route")
            raise HTTPException(403, "Access Denied")

        return args
    except NoResultFound:
        LOGGER.warning(f"component={args.source.id}: not found")
        raise HTTPException(403, "Access Denied")


@task_router.get("/")
async def get_task(
    task_request: TaskRequest,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(
        f"component={args.source.id}: request task with artifact={task_request.artifact_id} and job={task_request.job_id}"
    )

    jms: JobManagementService = JobManagementService(
        args.session,
        args.self_component,
    )

    return await jms.get_task_by_job_id(task_request.job_id)


@task_router.post("/")
async def post_task(
    task: Task,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.source.id}: new task execution")

    tms: TaskManagementService = TaskManagementService(
        args.session,
        args.source,
        args.exc.transfer_private_key(),
        args.exc.transfer_public_key(),
    )

    status = await tms.start_task(task, args.source.id)

    LOGGER.info(f"component={args.source.id}: executing {status}")


@task_router.post("/metrics")
async def post_metrics(
    metrics: Metrics,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(
        f"component={args.source.id}: submitted new metrics for "
        f"artifact={metrics.artifact_id} source={metrics.source}"
    )

    jms: JobManagementService = JobManagementService(
        args.session,
        args.self_component,
    )

    await jms.metrics(metrics)

    return


@task_router.post("/done")
async def post_done(
    done: TaskDone,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.source.id}: job={done.job_id} completed")
    jms: JobManagementService = JobManagementService(
        args.session,
        args.self_component,
    )
    tms: TaskManagementService = TaskManagementService(
        args.session,
        args.self_component,
        args.exc.transfer_private_key(),
        args.exc.transfer_public_key(),
    )

    try:
        async with args.lock:
            await jms.task_completed(done.job_id)
            await jms.check(done.artifact_id)
            await tms.check(done.artifact_id)

    except Exception as e:
        LOGGER.error(
            f"component={args.self_component.id}: something went wrong with completion of job={done.job_id} "
            f"from component={args.source.id}"
        )
        LOGGER.exception(e)
        return HTTPException(500)


@task_router.post("/error")
async def post_error(
    error: TaskError,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.warning(f"component={args.source.id}: job={error.job_id} in error={error.message}")
    jms: JobManagementService = JobManagementService(
        args.session,
        args.self_component,
    )

    try:
        await jms.task_failed(error)

    except Exception as e:
        LOGGER.error(f"component={args.source.id}: could not save error for job={error.job_id}")
        LOGGER.exception(e)
        return HTTPException(500)
