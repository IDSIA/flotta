from fastapi import APIRouter, Depends, Request, HTTPException, UploadFile

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...database import get_db, crud
from ...database.tables import Client, ClientToken
from ...database.settings import KeyValueStore, KEY_TOKEN_EXPIRATION
from ..actions import ActionManager
from ..schemas.client import *
from ..security import decrypt, generate_token, get_server_public_key, encrypt, get_client_public_key, check_token

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

        client = crud.create_client(db, client)
        client_token = crud.create_client_token(db, client_token)

        crud.create_client_event(db, client_id, 'creation')

        client_public_key: bytes = get_client_public_key(client)

        LOGGER.info(f'client_id={client_id}: joined')

        return ClientJoinResponse(
            id=encrypt(client_public_key, client_id),
            token=encrypt(client_public_key, token),
            public_key=get_server_public_key(db)
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

    client = crud.get_client_by_id(db, client_id)
    client_pub_key = get_client_public_key(client)

    # consume current results (if present) and compute next action

    payload = decrypt(db, request.payload)

    action, data = ActionManager().next(db, client, payload)

    LOGGER.info(f'client_id={client_id}: sending action={action}')

    crud.create_client_event(db, client_id, f'action:{action}')

    payload = {
        'action': action,
        'data': data,
    }

    return ClientUpdateResponse(
        payload=encrypt(client_pub_key, json.dumps(payload))
    )


@client_router.get('/client/update/files')
async def client_update_model(request: ClientUpdateModelRequest, db: Session = Depends(get_db), client_id: Client = Depends(check_token)):
    payload = json.loads(decrypt(db, request.payload))

    if 'model_id' in payload:
        model_id = payload['model_id']

        LOGGER.info(f'client_id={client_id}: requested model={model_id}')
        # TODO: send model_id related files
    
    if 'client_version' in payload:
        client_version = payload['client_version']

        LOGGER.info(f'client_id={client_id}: requested new client version={client_version}')
        # TODO: send new client_version related files
    
    # TODO:
    raise ValueError()
