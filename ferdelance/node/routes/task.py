from ferdelance.const import TYPE_CLIENT, TYPE_NODE
from ferdelance.core.metrics import Metrics
from ferdelance.logging import get_logger
from ferdelance.node.middlewares import SignedAPIRoute, ValidSessionArgs, valid_session_args
from ferdelance.node.services import JobManagementService, TaskManagementService
from ferdelance.schemas.resources import ResourceRequest
from ferdelance.tasks.tasks import Task, TaskDone, TaskError, TaskRequest

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse

from sqlalchemy.exc import NoResultFound

import os

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


@task_router.get("/", response_model=Task)
async def get_task(
    task_request: TaskRequest,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(
        f"component={args.component.id}: request new task with artifact={task_request.artifact_id} and job={task_request.job_id}"
    )

    jms: JobManagementService = JobManagementService(
        args.session, args.component, args.security_service.get_private_key(), args.security_service.get_public_key()
    )

    return await jms.get_task_by_job_id(task_request.job_id)


@task_router.post("/")
async def post_task(
    task: Task,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.component.id}: new task execution")

    tms: TaskManagementService = TaskManagementService(args.session, args.component)

    status = await tms.task_start(task)

    LOGGER.info(f"component={args.component.id}: executing {status}")


@task_router.get("/resource", response_class=FileResponse)
async def get_resource(
    req: ResourceRequest,
    args: ValidSessionArgs = Depends(allow_access),
):
    resource_id = req.resource_id

    LOGGER.info(f"component={args.component.id}: request resource={resource_id}")
    jms: JobManagementService = JobManagementService(
        args.session, args.component, args.security_service.get_private_key(), args.security_service.get_public_key()
    )

    try:
        resource = await jms.load_resource(resource_id)

        # TODO: add check for who can download which kind of resource

        return FileResponse(resource.path)

    except HTTPException as e:
        raise e

    except NoResultFound as e:
        LOGGER.error(f"component={args.component.id}: Resource does not exists for resource={resource_id}")
        LOGGER.exception(e)
        raise HTTPException(404)

    except Exception as e:
        LOGGER.error(f"component={args.component.id}: {e}")
        LOGGER.exception(e)
        raise HTTPException(500)


@task_router.post("/resource", response_model=ResourceRequest)
async def post_resource(
    request: Request,
    args: ValidSessionArgs = Depends(allow_access),
):
    job_id = args.extra_headers["job_id"]
    component = args.component

    LOGGER.info(f"component={component.id}: completed work on job={job_id}")

    jms: JobManagementService = JobManagementService(
        args.session, component, args.security_service.get_private_key(), args.security_service.get_public_key()
    )

    try:
        resource = await jms.store_resource(job_id)

        if "file" in args.extra_headers and args.extra_headers["file"] == "attached":
            LOGGER.info(f"component={component.id}: decrypting resource file to path={resource.path}")
            await args.security_service.stream_decrypt_file(request, resource.path)

        elif os.path.exists(resource.path):
            LOGGER.info(f"component={component.id}: found local resource file at path={resource.path}")
            # TODO: allow overwrite?

        else:
            LOGGER.error(f"component={component.id}: expected file at path={resource.path} not found")
            raise HTTPException(404)

        return ResourceRequest(
            artifact_id=resource.artifact_id,
            job_id=job_id,
            resource_id=resource.id,
        )

    except HTTPException as e:
        raise e

    except Exception as e:
        LOGGER.error(f"component={component.id}: could not save resource to disk for job={job_id}")
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

    jms: JobManagementService = JobManagementService(
        args.session, args.component, args.security_service.get_private_key(), args.security_service.get_public_key()
    )

    await jms.metrics(metrics)

    return


@task_router.post("/done")
async def post_done(
    done: TaskDone,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.warn(f"component={args.component.id}: job={done.job_id} completed")
    jms: JobManagementService = JobManagementService(
        args.session, args.component, args.security_service.get_private_key(), args.security_service.get_public_key()
    )

    try:
        await jms.task_completed(done.job_id)
        await jms.check(done.artifact_id)

    except Exception as e:
        LOGGER.error(f"component={args.component.id}: could not save error to disk for job={done.job_id}")
        LOGGER.exception(e)
        return HTTPException(500)


@task_router.post("/error")
async def post_error(
    error: TaskError,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.warn(f"component={args.component.id}: job={error.job_id} in error={error.message}")
    jms: JobManagementService = JobManagementService(
        args.session, args.component, args.security_service.get_private_key(), args.security_service.get_public_key()
    )

    try:
        await jms.task_failed(error)

    except Exception as e:
        LOGGER.error(f"component={args.component.id}: could not save error to disk for job={error.job_id}")
        LOGGER.exception(e)
        return HTTPException(500)
