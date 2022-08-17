from sqlalchemy.orm import Session
from .tables import Client

import os
import logging

LOGGER = logging.getLogger(__name__)


def create_user(db: Session, client: Client) -> Client:
    LOGGER.info(f'creating new user with version={client.version}')

    exists = (
        db.query(Client.client_id)
            .filter(
                (Client.machine_mac_address == client.machine_mac_address) |
                (Client.machine_node == client.machine_node)
            )
            .first() is not None
    )
    if exists:
        raise ValueError('Client already exists')

    db.add(client)
    db.commit()
    db.refresh(client)

    return client
