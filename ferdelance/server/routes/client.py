from typing import Any
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import Response

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...database import get_db
from ...database.tables import Client, ClientApp, ClientToken, ClientTask
from ..services.actions import ActionService
from ..services.application import ClientAppService
from ..services.client import ClientService
from ..services.ctask import ClientTaskService
from ..services.datasource import DataSourceService
from ..services.security import SecurityService
from ..security import check_token
from ..folders import STORAGE_ARTIFACTS

from ferdelance_shared.schemas import ClientJoinRequest, ClientJoinData, DownloadApp, Metadata, UpdateExecute, Artifact, ArtifactTask

import aiofiles
import logging
import json
import os

LOGGER = logging.getLogger(__name__)


client_router = APIRouter()


@client_router.post('/client/join', response_class=Response)
async def client_join(request: Request, client: ClientJoinRequest, db: Session = Depends(get_db)):
    """API for new client joining."""
    cs: ClientService = ClientService(db)
    ss: SecurityService = SecurityService(db, None)

    try:
        ip_address = request.client.host

        client_token: ClientToken = ss.generate_token(client.system, client.mac_address, client.node)

        token = client_token.token
        client_id = client_token.client_id

        client = Client(
            client_id=client_id,
            version=client.version,
            public_key=client.public_key,
            machine_system=client.system,
            machine_mac_address=client.mac_address,
            machine_node=client.node,
            ip_address=ip_address,
        )

        ss.client = cs.create_client(client)
        client_token = cs.create_client_token(client_token)

        cs.create_client_event(client_id, 'creation')

        LOGGER.info(f'client_id={client_id}: joined')

        cjd = ClientJoinData(
            id=client_id,
            token=token,
            public_key=ss.get_server_public_key_str(),
        )

        return Response(ss.server_encrypt_content(json.dumps(cjd.dict())))

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception('Database error')
        raise HTTPException(500, 'Internal error')

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, 'Invalid client data')


@client_router.post('/client/leave')
async def client_leave(db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """API for existing client to be removed"""
    cs: ClientService = ClientService(db)

    LOGGER.info(f'client_id={client_id}: request to leave')

    cs.client_leave(client_id)
    cs.create_client_event(client_id, 'left')

    return {}


@client_router.get('/client/update', response_class=Response)
async def client_update(request: Request, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new artifact package
    - new client app package
    - nothing (keep alive)
    """
    acs: ActionService = ActionService(db)
    cs: ClientService = ClientService(db)
    ss: SecurityService = SecurityService(db, client_id)

    LOGGER.info(f'client_id={client_id}: update request')
    cs.create_client_event(client_id, 'update')

    # consume current results (if present) and compute next action
    data = await request.body()
    payload: dict[str, Any] = json.loads(ss.server_decrypt_content(data))

    action = acs.next(ss.client, payload)

    LOGGER.info(f'client_id={client_id}: sending action={action}')

    cs.create_client_event(client_id, f'action:{action}')

    return Response(ss.server_encrypt_content(json.dumps(action.dict())))


@client_router.get('/client/download/application', response_class=Response)
async def client_update_files(request: Request, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """
    API request by the client to get updated files. With this endpoint a client can:
    - update application software
    - obtain model files
    """
    cas: ClientAppService = ClientAppService(db)
    cs: ClientService = ClientService(db)
    ss: SecurityService = SecurityService(db, client_id)

    LOGGER.info(f'client_id={client_id}: update files request')
    cs.create_client_event(client_id, 'update files')

    body = await request.body()
    payload = DownloadApp(**json.loads(ss.server_decrypt_content(body)))

    new_app: ClientApp = cas.get_newest_app()

    if new_app.version != payload.version:
        LOGGER.warning(f'client_id={client_id} requested app version={payload.version} while latest version={new_app.version}')
        return HTTPException(400, 'Old versions are not permitted')

    cs.update_client(client_id, version=payload.version)

    LOGGER.info(f'client_id={client_id}: requested new client version={payload.version}')

    return ss.server_stream_encrypt_file(new_app.path)


# TODO: /client/download/model

@client_router.post('/client/update/metadata')
async def client_update_metadata(request: Request, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """Endpoint used by a client to send information regarding its metadata. These metadata includes:
    - data source available
    - summary (source, data type, min value, max value, standard deviation, ...) of features available for each data source

    Structure of expected JSON:
    ```
    {"datasources": [{
        "name": <string>,
        "type": <string>,
        "removed": <boolean>,
        "n_records": <integer>,
        "n_features": <integer>,
        "features": [{
            "name": <string>,
            "dtype": <string>,
            "v_min": <float or null>,
            "v_max": <float or null>,
            "v_std": <float or null>
        }]
    }]}
    ```
    """
    cs: ClientService = ClientService(db)
    dss: DataSourceService = DataSourceService(db)
    ss: SecurityService = SecurityService(db, client_id)

    LOGGER.info(f'client_id={client_id}: update metadata request')
    cs.create_client_event(client_id, 'update metadata')

    body = await request.body()
    metadata = Metadata(**json.loads(ss.server_decrypt_content(body)))

    dss.create_or_update_metadata(client_id, metadata)


@client_router.get('/client/task/', response_class=Response)
async def client_get_task(request: Request, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    cs: ClientService = ClientService(db)
    cts: ClientTaskService = ClientTaskService(db)
    dss: DataSourceService = DataSourceService(db)
    ss: SecurityService = SecurityService(db, client_id)

    LOGGER.info(f'client_id={client_id}: new task request')

    cs.create_client_event(client_id, 'schedule task')

    body = await request.body()
    payload = UpdateExecute(**json.loads(ss.server_decrypt_content(body)))

    task: ClientTask = cts.get_task_for_client(payload.client_task_id)

    if task is None:
        return HTTPException(404, 'Task does not exists')

    artifact_path = os.path.join(STORAGE_ARTIFACTS, f'{task.artifact_id}.json')

    if not os.path.exists(artifact_path):
        return HTTPException(404, 'Artifact does not exits')

    async with aiofiles.open(artifact_path, 'r') as f:
        data = await f.read()
        artifact = Artifact(**json.loads(data))

    client_datasource_ids = dss.get_datasource_ids_by_client_id(client_id)

    content = ArtifactTask(
        artifact_id=task.artifact_id,
        client_task_id=task.client_task_id,
        model=artifact.model,
        queries=[q for q in artifact.queries if q.datasources_id in client_datasource_ids]
    )

    return Response(ss.server_encrypt_content(json.dumps(content.dict())))
