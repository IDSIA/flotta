from sqlalchemy.orm import Session
from .tables import Client, ClientEvent

import os
import logging

LOGGER = logging.getLogger(__name__)


def create_client(db: Session, client: Client) -> Client:
    LOGGER.info(f'creating new user with version={client.version} mac_address={client.machine_mac_address} node={client.machine_node}')

    existing_client_id = (
        db.query(Client.client_id)
            .filter(
                (Client.machine_mac_address == client.machine_mac_address) |
                (Client.machine_node == client.machine_node)
            )
            .first()
    )
    if existing_client_id is not None:
        LOGGER.warning(f'client already exists with id {existing_client_id}')
        raise ValueError('Client already exists')

    db.add(client)
    db.commit()
    db.refresh(client)

    return client


def get_client_by_token(db: Session, token: str) -> Client:
    return db.query(Client).filter(Client.token == token).first()


def create_client_event(db: Session, client: Client, event: str) -> ClientEvent:
    LOGGER.info(f'creating new client_event for client_id={client.client_id} event="{event}"')

    db_client_event = ClientEvent(
        client_id=client.client_id,
        event=event
    )

    db.add(db_client_event)
    db.commit()
    db.refresh(db_client_event)

    return db_client_event


def get_all_client_events(db: Session, client: Client) -> list[ClientEvent]:
    LOGGER.info(f'requested all events for client_id={client.client_id}')

    return db.query(ClientEvent).filter(ClientEvent.client_id == client.client_id).all()
