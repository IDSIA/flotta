from flotta.config import config_manager
from flotta.const import TYPE_CLIENT, TYPE_NODE, TYPE_USER
from flotta.logging import get_logger
from flotta.node.middlewares import SignedAPIRoute, ValidSessionArgs, valid_session_args
from flotta.node.services.resource import ResourceManagementService
from flotta.schemas.resources import ResourceIdentifier

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse

from sqlalchemy.exc import NoResultFound

import os


LOGGER = get_logger(__name__)

resource_router = APIRouter(prefix="/resource", route_class=SignedAPIRoute)


async def allow_access(args: ValidSessionArgs = Depends(valid_session_args)) -> ValidSessionArgs:
    try:
        if args.source.type_name not in (TYPE_CLIENT, TYPE_NODE, TYPE_USER):
            LOGGER.warning(f"component={args.source.id}: type={args.source.type_name} cannot access this route")
            raise HTTPException(403, "Access Denied")

        return args

    except NoResultFound:
        LOGGER.warning(f"component={args.source.id}: not found")
        raise HTTPException(403, "Access Denied")


@resource_router.get("/")
async def get_task(
    res_id: ResourceIdentifier,
    args: ValidSessionArgs = Depends(allow_access),
):
    LOGGER.info(f"component={args.source.id}: request resource={res_id.resource_id}")

    if not config_manager.get().node.allow_resource_download:
        LOGGER.warning(f"component={args.source.id}: this node does not allow the download of resources")
        raise HTTPException(403, "Access Denied")

    rm: ResourceManagementService = ResourceManagementService(args.session)

    try:
        resource = await rm.load_resource(res_id)

        if resource.encrypted_for is not None and resource.encrypted_for != args.source.id:
            LOGGER.warn(
                f"component={args.source.id}: tried to fetch resource for another component={resource.encrypted_for}"
            )
            raise HTTPException(403, "Access Denied")

        # this is for the middleware
        if resource.encrypted_for is not None:
            headers = {"Encrypted_for": resource.encrypted_for}
        else:
            headers = {}

        return FileResponse(path=resource.path, headers=headers)

    except NoResultFound as e:
        LOGGER.error(f"component={args.source.id}: resource={res_id.resource_id} not found")
        LOGGER.exception(e)
        raise HTTPException(404)

    except Exception as e:
        LOGGER.error(f"component={args.source.id}: {e}")
        LOGGER.exception(e)
        raise HTTPException(500)


@resource_router.post("/")
async def post_resource(
    request: Request,
    args: ValidSessionArgs = Depends(allow_access),
):
    component = args.source

    rm: ResourceManagementService = ResourceManagementService(args.session)

    try:
        # get resource from db
        if args.source.type_name == TYPE_USER:
            LOGGER.info(f"component={component.id}: adding external resource")
            resource = await rm.create_resource_external(component.id)

        else:
            if "job_id" not in args.extra_headers:
                LOGGER.error(f"component={component.id}: missing header job_id")
                raise HTTPException(404, "Not Found")

            job_id = args.extra_headers["job_id"]

            LOGGER.info(f"component={component.id}: adding resource as product of job={job_id} for {args.target.id}")

            resource = await rm.store_resource(job_id, args.source.id)

            if args.target.id != args.self_component.id:
                # proxy
                LOGGER.info(f"component={component.id}: proxy resource from {args.source.id} to {args.target.id}")
                resource = await rm.set_encrypted_for(resource, args.target.id)

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
