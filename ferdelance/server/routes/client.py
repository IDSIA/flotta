from fastapi import APIRouter, Depends, Request, HTTPException, UploadFile

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...database import get_db, crud
from ...database.tables import Client
from ..schemas.client import *
from ..security import generate_token, get_server_public_key, encrypt, get_client_public_key, check_token

import logging

LOGGER = logging.getLogger(__name__)


client_router = APIRouter()


@client_router.post('/client/join', response_model=ClientJoinResponse)
async def client_join(request: Request, client: ClientJoinRequest, db: Session=Depends(get_db)):
    """API for new client joining."""
    try:
        ip_address = request.client.host

        token, client_id = generate_token(client)

        client_public_key: bytes = get_client_public_key(client)

        db_client = Client(
            client_id=client_id,
            version=client.version,
            public_key=client.public_key,
            machine_system=client.system,
            machine_mac_address=client.mac_address,
            machine_node=client.node,
            token=token,
            ip_address=ip_address
        )

        db_client = crud.create_client(db, db_client)

        crud.create_client_event(db, db_client, 'creation')

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


@client_router.post('/client/leave', response_model=ClientLeaveResponse, dependencies=[Depends(check_token)])
async def client_leave(client: ClientLeaveRequest, db: Session=Depends(get_db)):
    """API for existing client to be removed"""

    # check that client exists

    # Delete it

    return


@client_router.get('/client/update', response_model=ClientUpdateResponse)
async def client_update(client: ClientUpdateRequest, db: Session=Depends(get_db), db_client: Client=Depends(check_token)):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new algorithm package
    - new client package
    - nothing (keep alive)
    """

    LOGGER.info(f'keep alive from client_id={db_client.client_id}')
    crud.create_client_event(db, db_client, 'update')

    return action_nothing(db, db_client)


def action_nothing(db: Session, client: Client): 
        # action = nothing ------------------------------------------------------
    LOGGER.info(f'sending action=nothing to client_id={client.client_id}')
    crud.create_client_event(db, client, 'action:nothing')

    return ClientUpdateResponse(
        action='nothing',
        endpoint='/client/update',
    )

