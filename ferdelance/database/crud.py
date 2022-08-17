from sqlalchemy.orm import Session
from .tables import Client, ClientEvent

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


def create_client_event(db: Session, client: Client, event: str) -> ClientEvent:
    LOGGER.info(f'creating new client_event for client_id={client.client_id} event={event}')

    db_client_event = ClientEvent(
        client_id=client.client_id,
        event=event
    )

    db.add(db_client_event)
    db.commit()
    db.refresh(db_client_event)

    return db_client_event
