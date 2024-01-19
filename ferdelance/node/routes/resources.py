from ferdelance.config import config_manager
from ferdelance.const import TYPE_CLIENT, TYPE_NODE, TYPE_USER
from ferdelance.logging import get_logger
from ferdelance.node.middlewares import SignedAPIRoute, ValidSessionArgs, valid_session_args
from ferdelance.node.services.resource import ResourceManagementService
from ferdelance.schemas.resources import ResourceIdentifier

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse

from sqlalchemy.exc import NoResultFound

import os

LOGGER = get_logger(__name__)

resource_router = APIRouter(prefix="/resource", route_class=SignedAPIRoute)


async def allow_access(args: ValidSessionArgs = Depends(valid_session_args)) -> ValidSessionArgs:
    try:
        if args.component.type_name not in (TYPE_CLIENT, TYPE_NODE, TYPE_USER):
            LOGGER.warning(f"component={args.component.id}: type={args.component.type_name} cannot access this route")

        return args

    except NoResultFound:
        LOGGER.warning(f"component={args.component.id}: not found")
        raise HTTPException(403)


@resource_router.get("/")
async def get_task(
    res_id: ResourceIdentifier,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.component.id}: request resource={res_id.resource_id}")

    if not config_manager.get().node.allow_resource_download:
        LOGGER.warning(f"component={args.component.id}: this node does not allow the download of resources")
        raise HTTPException(403)

    rm: ResourceManagementService = ResourceManagementService(args.session)

    try:
        resource = await rm.load_resource(res_id)

        return FileResponse(resource.path)

    except NoResultFound as e:
        LOGGER.error(f"component={args.component.id}: resource={res_id.resource_id} not found")
        LOGGER.exception(e)
        raise HTTPException(404)

    except Exception as e:
        LOGGER.error(f"component={args.component.id}: {e}")
        LOGGER.exception(e)
        raise HTTPException(500)


@resource_router.post("/")
async def post_resource(
    request: Request,
    args: ValidSessionArgs = Depends(allow_access),
):
    component = args.component

    rm: ResourceManagementService = ResourceManagementService(args.session)

    try:
        # get resource from db
        if args.component.type_name == TYPE_USER:
            LOGGER.info(f"component={component.id}: adding external resource")
            resource = await rm.create_resource_external(component.id)

        else:
            if "job_id" not in args.extra_headers:
                LOGGER.error(f"component={component.id}: missing header job_id")
                raise HTTPException(403)

            job_id = args.extra_headers["job_id"]

            LOGGER.info(f"component={component.id}: adding resource as product of job={job_id}")

            resource = await rm.store_resource(job_id, args.component.id)

        # use resource's path
        if "file" in args.extra_headers and args.extra_headers["file"] == "attached":
            LOGGER.info(f"component={component.id}: decrypting resource file to path={resource.path}")
            await args.exc.stream_decrypt_file(request.stream(), resource.path)

        elif os.path.exists(resource.path):
            LOGGER.info(f"component={component.id}: found local resource file at path={resource.path}")
            # TODO: allow overwrite?

        else:
            LOGGER.error(f"component={component.id}: expected file at path={resource.path} not found")
            raise HTTPException(404)

        return ResourceIdentifier(
            producer_id=component.id,
            resource_id=resource.id,
        )

    except HTTPException as e:
        raise e

    except Exception as e:
        LOGGER.error(f"component={component.id}: could not save resource to disk")
        LOGGER.exception(e)
        raise HTTPException(500)


# TODO: add endpoint to find the location of a resource (the node who has it)
