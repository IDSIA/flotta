from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import StreamingResponse

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...database import get_db, crud
from ...database.tables import Client, ClientApp, ClientToken
from ...database.settings import KeyValueStore, KEY_TOKEN_EXPIRATION
from ..actions import ActionManager
from ..schemas.client import *
from ..security import (
    generate_token,
    check_token,
    server_decrypt,
    server_encrypt,
    get_server_public_key_str,
    server_stream_encrypt,
)

import logging
import json

LOGGER = logging.getLogger(__name__)


client_router = APIRouter()


@client_router.post('/client/join', response_model=ClientJoinResponse)
async def client_join(request: Request, client: ClientJoinRequest, db: Session = Depends(get_db)):
    """API for new client joining."""
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

        client: Client = crud.create_client(db, client)
        client_token = crud.create_client_token(db, client_token)

        crud.create_client_event(db, client_id, 'creation')

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
async def client_leave(client: ClientLeaveRequest, db: Session = Depends(get_db), client_id: Client = Depends(check_token)):
    """API for existing client to be removed"""

    LOGGER.info(f'client_id={client_id}: request to leave')

    crud.client_leave(db, client_id)
    crud.create_client_event(db, client_id, 'left')

    return ClientLeaveResponse()


@client_router.get('/client/update', response_model=ClientUpdateResponse)
async def client_update(request: ClientUpdateRequest, db: Session = Depends(get_db), client_id: Client = Depends(check_token)):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new artifact package
    - new client app package
    - nothing (keep alive)
    """

    LOGGER.info(f'client_id={client_id}: update request')
    crud.create_client_event(db, client_id, 'update')

    client: Client = crud.get_client_by_id(db, client_id)

    # consume current results (if present) and compute next action
    payload: str = server_decrypt(db, request.payload)

    action, data = ActionManager().next(db, client, payload)

    LOGGER.info(f'client_id={client_id}: sending action={action}')

    crud.create_client_event(db, client_id, f'action:{action}')

    payload = {
        'action': action,
        'data': data,
    }

    return ClientUpdateResponse(
        payload=server_encrypt(client, json.dumps(payload))
    )


@client_router.get('/client/update/files')
def client_update_files(request: ClientUpdateModelRequest, db: Session = Depends(get_db), client_id: Client = Depends(check_token)):
    payload = json.loads(server_decrypt(db, request.payload))
    client = crud.get_client_by_id(db, client_id)

    if 'client_version' in payload:
        client_version = payload['client_version']

        new_app: ClientApp = crud.get_newest_app(db)

        if new_app.version != client_version:
            LOGGER.warning(f'client_id={client_id} requested app version={client_version} while latest version={new_app.version}')
            return HTTPException(400)

        crud.update_client(db, client_id, version=client_version)

        LOGGER.info(f'client_id={client_id}: requested new client version={client_version}')

        return server_stream_encrypt(client, new_app.path)

    if 'model_id' in payload:
        model_id = payload['model_id']

        LOGGER.info(f'client_id={client_id}: requested model={model_id}')
        # TODO: send model_id related files

    LOGGER.info(f'client_id={client_id}: requested an invalid file with payload={payload}')
    raise HTTPException(404)
