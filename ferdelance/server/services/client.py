from . import DBSessionService, Session

from ...database.tables import Client, ClientEvent, ClientToken, ClientApp, Model

import logging

LOGGER = logging.getLogger(__name__)


class ClientService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def create_client(self, client: Client) -> Client:
        LOGGER.info(f'creating new client with version={client.version} mac_address={client.machine_mac_address} node={client.machine_node}')

        existing_client_id = (
            self.db.query(Client.client_id)
            .filter(
                (Client.machine_mac_address == client.machine_mac_address) |
                (Client.machine_node == client.machine_node)
            )
            .first()
        )
        if existing_client_id is not None:
            LOGGER.warning(f'client already exists with id {existing_client_id}')
            raise ValueError('Client already exists')

        self.db.add(client)
        self.db.commit()
        self.db.refresh(client)

        return client

    def update_client(self, client_id: str, version: str = None) -> None:
        u = dict()

        if version is not None:
            LOGGER.info(f'client_id={client_id}: update version to {version}')
            u['version'] = version

        if not u:
            return

        self.db.query(Client).filter(Client.client_id == client_id).update(u)
        self.db.commit()

    def client_leave(self, client_id: str) -> Client:
        self.db.query(Client).filter(Client.client_id == client_id).update({
            'active': False,
            'left': True,
        })
        self.invalidate_all_tokens(client_id)  # this will already commit the changes!

    def get_client_by_id(self, client_id: str) -> Client:
        return self.db.query(Client).filter(Client.client_id == client_id).first()

    def get_client_list(self) -> list[Client]:
        return self.db.query(Client).all()

    def get_client_by_token(self, token: str) -> Client:
        return self.db.query(Client)\
            .join(ClientToken, Client.client_id == ClientToken.token_id)\
            .filter(ClientToken.token == token)\
            .first()

    def create_client_token(self, token: ClientToken) -> ClientToken:
        LOGGER.info(f'client_id={token.client_id}: creating new token')

        existing_client_id = self.db.query(ClientToken.client_id).filter(ClientToken.token == token.token).first()

        if existing_client_id is not None:
            LOGGER.warning(f'valid token already exists for client_id {existing_client_id}')
            # TODO: check if we have more strong condition for this
            return

        self.db.add(token)
        self.db.commit()
        self.db.refresh(token)

        return token

    def invalidate_all_tokens(self, client_id: str) -> None:
        self.db.query(ClientToken).filter(ClientToken.client_id == client_id).update({
            'valid': False,
        })
        self.db.commit()

    def get_client_id_by_token(self, token: str) -> str:
        return self.db.query(ClientToken.client_id).filter(ClientToken.token == token).first()

    def get_client_token_by_token(self, token: str) -> ClientToken:
        return self.db.query(ClientToken).filter(ClientToken.token == token).first()

    def get_client_token_by_client_id(self, client_id: str) -> ClientToken:
        return self.db.query(ClientToken).filter(ClientToken.client_id == client_id).first()

    def create_client_event(self, client_id: str, event: str) -> ClientEvent:
        LOGGER.info(f'client_id={client_id}: creating new event="{event}"')

        db_client_event = ClientEvent(
            client_id=client_id,
            event=event
        )

        self.db.add(db_client_event)
        self.db.commit()
        self.db.refresh(db_client_event)

        return db_client_event

    def get_all_client_events(self, client: Client) -> list[ClientEvent]:
        LOGGER.info(f'client_id={client.client_id}: requested all events')

        return self.db.query(ClientEvent).filter(ClientEvent.client_id == client.client_id).all()