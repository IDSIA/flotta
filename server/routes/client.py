from fastapi import APIRouter, Depends, Request, HTTPException

from database import Session, get_db
from database.tables import Client
from ..schemas.client import *
from ..security import generate_token, get_server_public_key, encrypt, get_client_public_key

import logging

LOGGER = logging.getLogger(__name__)

client_router = APIRouter()


@client_router.post('/client/join', response_model=ClientJoinResponse)
async def client_join(request: Request, client: ClientJoinRequest, db: Session=Depends(get_db)):
    """API for new client joining."""
    try:
        ip_address = request.client.host

        token, client_uuid = generate_token(client)

        client_public_key: bytes = get_client_public_key(client)

        db_client = Client(
            version=client.version,
            public_key=client.public_key,
            uuid=client_uuid,
            machine_system=client.system,
            machine_mac_address=client.mac_address,
            machine_node=client.node,
            token=token,
            ip_address=ip_address
        )

        db.add(db_client)
        db.commit()

        return ClientJoinResponse(
            uuid=encrypt(client_public_key, client_uuid),
            token=encrypt(client_public_key, token),
            public_key=get_server_public_key(db)
        )

    except Exception as e:
        LOGGER.exception(e)
        LOGGER.error('Client already exists')
        raise HTTPException(403)


@client_router.post('/client/leave', response_model=ClientLeaveResponse)
async def client_leave(client: ClientLeaveRequest, db: Session=Depends(get_db)):
    """API for existing client to be removed"""

    # check that client exists

    # Delete it

    return


@client_router.get("/client/update", response_model=ClientUpdateResponse)
async def client_update(client: ClientUpdateRequest, db: Session=Depends(get_db)):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new algorithm package
    - new client package
    """

    return
