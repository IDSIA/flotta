import logging

from sqlalchemy import select

from ..schemas import Client as ClientView
from ..tables import Client, ClientEvent, ClientToken
from .core import AsyncSession, DBSessionService

LOGGER = logging.getLogger(__name__)


def get_view(client: Client) -> ClientView:
    return ClientView(
        client_id=client.client_id,
        version=client.version,
        public_key=client.public_key,
        machine_mac_address=client.machine_mac_address,
        machine_system=client.machine_system,
        machine_node=client.machine_node,
        type=client.type,
        active=client.active,
        blacklisted=client.blacklisted,
        left=client.left,
        ip_address=client.ip_address,
    )


class ClientService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_client(
        self,
        client_id: str,
        version: str,
        public_key: str,
        machine_system: str,
        machine_mac_address: str,
        machine_node: str,
        ip_address: str,
        type: str = "CLIENT",
    ) -> ClientView:
        client = Client(
            client_id=client_id,
            version=version,
            public_key=public_key,
            machine_system=machine_system,
            machine_mac_address=machine_mac_address,
            machine_node=machine_node,
            ip_address=ip_address,
            type=type,
        )

        LOGGER.info(
            f"client_id={client.client_id}: creating new client with version={client.version} mac_address={client.machine_mac_address} node={client.machine_node} type={client.type}"
        )

        res = await self.session.execute(
            select(Client.client_id)
            .where(
                (Client.machine_mac_address == client.machine_mac_address)
                | (Client.machine_node == client.machine_node)
            )
            .limit(1)
        )
        existing_client_id = res.scalar_one_or_none()
        if existing_client_id is not None:
            LOGGER.warning(f"client_id={existing_client_id}: client already exists")
            raise ValueError("Client already exists")

        self.session.add(client)
        await self.session.commit()
        await self.session.refresh(client)

        return get_view(client)

    async def update_client(self, client_id: str, version: str = "") -> None:
        if not version:
            LOGGER.warning("cannot update a version with an empty string")
            return

        client: Client = await self.session.scalar(
            select(Client).where(Client.client_id == client_id)
        )
        client.version = version

        await self.session.commit()

        LOGGER.info(f"client_id={client_id}: updated client version to {version}")

    async def client_leave(self, client_id: str) -> None:
        await self.invalidate_all_tokens(client_id)

        client: Client = await self.session.scalar(
            select(Client).where(Client.client_id == client_id)
        )
        client.active = False
        client.left = True

        await self.session.commit()

    async def get_client_by_id(self, client_id: str) -> ClientView:
        res = await self.session.execute(
            select(Client).where(Client.client_id == client_id)
        )
        return get_view(res.scalar_one())

    async def get_client_by_key(self, public_key: str) -> ClientView:
        res = await self.session.execute(
            select(Client).where(Client.public_key == public_key)
        )
        return get_view(res.scalar_one())

    async def get_client_list(self) -> list[ClientView]:
        res = await self.session.scalars(select(Client).where(Client.type == "CLIENT"))
        return [get_view(c) for c in res.all()]

    async def get_client_by_token(self, token: str) -> ClientView:
        res = await self.session.execute(
            select(Client)
            .join(ClientToken, Client.client_id == ClientToken.token_id)
            .where(ClientToken.token == token)
        )
        return get_view(res.scalar_one())

    async def create_client_token(self, token: ClientToken) -> ClientToken:
        LOGGER.info(f"client_id={token.client_id}: creating new token")

        res = await self.session.execute(
            select(ClientToken).where(ClientToken.token == token.token)
        )

        existing_client_token: ClientToken | None = res.scalar_one_or_none()

        if existing_client_token is not None:
            LOGGER.warning(
                f"client_id={existing_client_token.client_id}: a valid token already exists"
            )
            # TODO: check if we have more strong condition for this
            return existing_client_token

        self.session.add(token)
        await self.session.commit()
        await self.session.refresh(token)

        return token

    async def invalidate_all_tokens(self, client_id: str) -> None:
        res = await self.session.scalars(
            select(ClientToken).where(ClientToken.client_id == client_id)
        )
        tokens: list[ClientToken] = res.all()

        for token in tokens:
            token.valid = False

        await self.session.commit()

    async def get_client_id_by_token(self, token: str) -> str | None:
        res = await self.session.scalars(
            select(ClientToken).where(ClientToken.token == token)
        )
        client_token: ClientToken | None = res.one_or_none()

        if client_token is None:
            return None

        return str(client_token.client_id)

    async def get_client_token_by_token(self, token: str) -> ClientToken | None:
        res = await self.session.execute(
            select(ClientToken).where(ClientToken.token == token)
        )
        return res.scalar_one_or_none()

    async def get_client_token_by_client_id(self, client_id: str) -> ClientToken | None:
        res = await self.session.execute(
            select(ClientToken).where(
                ClientToken.client_id == client_id, ClientToken.valid == True
            )
        )
        return res.scalar_one_or_none()

    async def create_client_event(self, client_id: str, event: str) -> ClientEvent:
        LOGGER.debug(f'client_id={client_id}: creating new event="{event}"')

        session_client_event = ClientEvent(client_id=client_id, event=event)

        self.session.add(session_client_event)
        await self.session.commit()
        await self.session.refresh(session_client_event)

        return session_client_event

    async def get_all_client_events(self, client: Client) -> list[ClientEvent]:
        res = await self.session.scalars(
            select(ClientEvent).where(ClientEvent.client_id == client.client_id)
        )
        return res.all()

    async def get_token_by_client_type(self, client_type: str) -> str | None:
        client_token: ClientToken | None = await self.session.scalar(
            select(ClientToken)
            .join(Client, Client.client_id == ClientToken.client_id)
            .where(Client.type == client_type)
            .limit(1)
        )

        if client_token is None:
            return None

        return str(client_token.token)
