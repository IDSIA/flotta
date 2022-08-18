from fastapi import APIRouter, Depends, Request, HTTPException, UploadFile

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...database import get_db, crud
from ...database.tables import Client, ClientToken
from ..schemas.client import *
from ..security import generate_token, get_server_public_key, encrypt, get_client_public_key, check_token

import logging
import json

LOGGER = logging.getLogger(__name__)


client_router = APIRouter()


@client_router.post('/client/join', response_model=ClientJoinResponse)
async def client_join(request: Request, client: ClientJoinRequest, db: Session=Depends(get_db)):
    """API for new client joining."""
    try:
        ip_address = request.client.host

        client_token: ClientToken = generate_token(client)
        client_public_key: bytes = get_client_public_key(client)

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
async def client_leave(client: ClientLeaveRequest, db: Session=Depends(get_db), client_id: Client=Depends(check_token)):
    """API for existing client to be removed"""

    LOGGER.info(f'leave requerst for client_id={client_id}')

    crud.client_leave(db, client_id)
    crud.create_client_event(db, client_id, 'left')

    return ClientLeaveResponse()


@client_router.get('/client/update', response_model=ClientUpdateResponse)
async def client_update(client: ClientUpdateRequest, db: Session=Depends(get_db), client_id: Client=Depends(check_token)):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new algorithm package
    - new client package
    - nothing (keep alive)
    """

    LOGGER.info(f'keep alive from client_id={client_id}')
    crud.create_client_event(db, client_id, 'update')

    client = crud.get_client_by_id(db, client_id)
    public_key = get_client_public_key(client)
    payload = action_nothing(db, client_id)

    return ClientUpdateResponse(
        payload=encrypt(public_key, json.dumps(payload))
    )


def action_nothing(db: Session, client_id: str) -> dict[str, str]:
    """When a client receives this action, he does nothing and waits for the next update request."""

    # TODO: move actions in another file!

    LOGGER.info(f'sending action=nothing to client_id={client_id}')
    crud.create_client_event(db, client_id, 'action:nothing')

    return{
        'action': 'nothing',
        'endpoint': '/client/update',
    }

