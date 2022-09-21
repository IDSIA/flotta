from typing import Any
from fastapi import APIRouter, Depends, Request, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...database import get_db
from ...database.tables import Client, ClientApp, ClientToken
from ...database.settings import KeyValueStore, KEY_TOKEN_EXPIRATION
from ..services.actions import ActionService
from ..services.application import ClientAppService
from ..services.client import ClientService
from ..services.ctask import ClientTaskService
from ..services.datasource import DataSourceService
from ..schemas.client import *
from ..schemas.workbench import ArtifactSubmitRequest, QueryRequest
from ..security import (
    generate_token,
    check_token,
    server_decrypt,
    server_encrypt,
    get_server_public_key_str,
    server_stream_encrypt,
    server_stream_decrypt_to_dictionary,
    server_memory_stream_encrypt,
)
from ..folders import STORAGE_ARTIFACTS

import aiofiles
import logging
import json
import os

LOGGER = logging.getLogger(__name__)


client_router = APIRouter()


@client_router.post('/client/join', response_model=ClientJoinResponse)
async def client_join(request: Request, client: ClientJoinRequest, db: Session = Depends(get_db)):
    """API for new client joining."""
    cs: ClientService = ClientService(db)

    try:
        ip_address = request.client.host

        kvs = KeyValueStore(db)
        token_exp: int = kvs.get_int(KEY_TOKEN_EXPIRATION)

        client_token: ClientToken = generate_token(client.system, client.mac_address, client.node, exp_time=token_exp)

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

        client: Client = cs.create_client(client)
        client_token = cs.create_client_token(client_token)

        cs.create_client_event(client_id, 'creation')

        LOGGER.info(f'client_id={client_id}: joined')

        return ClientJoinResponse(
            id=server_encrypt(client, client_id),
            token=server_encrypt(client, token),
            public_key=get_server_public_key_str(db)
        )

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception('Database error')
        raise HTTPException(500, detail='Internal error')

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, detail='Invalid client data')


@client_router.post('/client/leave', response_model=ClientLeaveResponse)
async def client_leave(client: ClientLeaveRequest, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """API for existing client to be removed"""
    cs: ClientService = ClientService(db)

    LOGGER.info(f'client_id={client_id}: request to leave')

    cs.client_leave(client_id)
    cs.create_client_event(client_id, 'left')

    return ClientLeaveResponse()


@client_router.get('/client/update', response_model=ClientUpdateResponse)
async def client_update(request: ClientUpdateRequest, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new artifact package
    - new client app package
    - nothing (keep alive)
    """
    acs: ActionService = ActionService(db)
    cs: ClientService = ClientService(db)

    LOGGER.info(f'client_id={client_id}: update request')
    cs.create_client_event(client_id, 'update')

    client: Client = cs.get_client_by_id(client_id)

    # consume current results (if present) and compute next action
    payload: str = server_decrypt(db, request.payload)

    action, data = acs.next(client, payload)

    LOGGER.info(f'client_id={client_id}: sending action={action}')

    cs.create_client_event(client_id, f'action:{action}')

    payload = {
        'action': action,
        'data': data,
    }

    return ClientUpdateResponse(
        payload=server_encrypt(client, json.dumps(payload))
    )


@client_router.get('/client/update/files')
async def client_update_files(request: ClientUpdateModelRequest, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """
    API request by the client to get updated files. With this endpoint a client can:
    - update application software
    - obtain model files
    """
    cas: ClientAppService = ClientAppService(db)
    cs: ClientService = ClientService(db)

    LOGGER.info(f'client_id={client_id}: update files request')
    cs.create_client_event(client_id, 'update files')

    payload = json.loads(server_decrypt(db, request.payload))
    client = cs.get_client_by_id(client_id)

    if 'client_version' in payload:
        client_version = payload['client_version']

        new_app: ClientApp = cas.get_newest_app()

        if new_app.version != client_version:
            LOGGER.warning(f'client_id={client_id} requested app version={client_version} while latest version={new_app.version}')
            return HTTPException(400)

        cs.update_client(client_id, version=client_version)

        LOGGER.info(f'client_id={client_id}: requested new client version={client_version}')

        return server_stream_encrypt(client, new_app.path)

    if 'model_id' in payload:
        model_id = payload['model_id']

        LOGGER.info(f'client_id={client_id}: requested model={model_id}')
        # TODO: send model_id related files

    LOGGER.info(f'client_id={client_id}: requested an invalid file with payload={payload}')
    raise HTTPException(404)


@client_router.post('/client/update/metadata')
async def client_update_metadata(file: UploadFile, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
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

    LOGGER.info(f'client_id={client_id}: update metadata request')
    cs.create_client_event(client_id, 'update metadata')

    metadata = server_stream_decrypt_to_dictionary(file, db)

    for ds in metadata['datasources']:
        dss.create_or_update_datasource(client_id, ds)


@client_router.get('/client/task/{client_task_id}', response_class=StreamingResponse)
async def client_get_task(request: ClientTaskRequest, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    cs: ClientService = ClientService(db)
    dss: DataSourceService = DataSourceService(db)

    LOGGER.info(f'client_id={client_id}: new task request')

    cts: ClientTaskService = ClientTaskService(db)
    cs.create_client_event(client_id, 'schedule task')

    client: Client = cs.get_client_by_id(client_id)

    payload = json.loads(server_decrypt(db, request.payload))

    client_task_id: str = payload['client_task_id']

    task = cts.get_task_for_client(client_task_id)  # TODO: sanitize input?

    artifact_id: str = task.artifact_id

    artifact_path = os.path.join(STORAGE_ARTIFACTS, f'{artifact_id}.json')

    async with aiofiles.open(artifact_path, 'r') as f:
        artifact = ArtifactSubmitRequest(**json.load(f))

    client_datasource_ids = [ds.datasource_id for ds in dss.get_datasource_by_client_id(client)]

    query: QueryRequest = artifact.query

    # TODO: this should be an object in a schema
    content: dict[str, Any] = {
        'artifact_id': artifact_id,
        'features': [f for f in query.features if f.datasource_id in client_datasource_ids],
        'filters':  [f for f in query.filters if f.feature.datasource_id in client_datasource_ids],
        'transformers':  [t for t in query.transformers if t.feature.datasource_id in client_datasource_ids],
    }

    data_to_send = server_memory_stream_encrypt(json.dumps(content), client)

    return StreamingResponse(data_to_send)
