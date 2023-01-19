from ferdelance.database.schemas import Component as ComponentView, User
from ferdelance.database.tables import (
    Component as ComponentDB,
    Event,
    Token,
    ComponentType,
)
from ferdelance.database.services.core import AsyncSession, DBSessionService
from ferdelance.database.data import TYPE_CLIENT, TYPE_WORKBENCH

from sqlalchemy import select

import logging

LOGGER = logging.getLogger(__name__)


def view(component: ComponentDB) -> ComponentView:
    return ComponentView(**component.__dict__)

def viewUser(component: ComponentDB) -> User:
    return User(**component.__dict__)


class ComponentService(DBSessionService):
    """Service used to manage components."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_types(self, type_names: list[str]):
        dirty: bool = False

        for type_name in type_names:

            res = await self.session.execute(
                select(ComponentType).where(ComponentType.type == type_name).limit(1)
            )

            existing_type = res.scalar_one_or_none()
            if existing_type is None:
                tn = ComponentType(type=type_name)
                self.session.add(tn)
                dirty =True

        if dirty:
            await self.session.commit()

    async def create_client(
        self,
        client_id: str,
        version: str,
        public_key: str,
        machine_system: str,
        machine_mac_address: str,
        machine_node: str,
        ip_address: str,
    ) -> ComponentView:
        component = ComponentDB(
            component_id=client_id,
            version=version,
            public_key=public_key,
            machine_system=machine_system,
            machine_mac_address=machine_mac_address,
            machine_node=machine_node,
            ip_address=ip_address,
            type=TYPE_CLIENT,
        )

        LOGGER.info(
            f"component_id={component.component_id}: creating new client version={component.version} mac_address={component.machine_mac_address} node={component.machine_node} type={component.type}"
        )

        res = await self.session.execute(
            select(ComponentDB.component_id)
            .where(
                (ComponentDB.machine_mac_address == component.machine_mac_address)
                | (ComponentDB.machine_node == component.machine_node)
            )
            .limit(1)
        )
        existing_client_id = res.scalar_one_or_none()
        if existing_client_id is not None:
            LOGGER.warning(f"client_id={existing_client_id}: client already exists")
            raise ValueError("Client already exists")

        self.session.add(component)
        await self.session.commit()
        await self.session.refresh(component)

        return view(component)

    async def create_user(self, user_id: str, public_key: str) -> User:
        LOGGER.info(f"user_id={user_id}: creating new user")

        res = await self.session.execute(
            select(User.user_id).where(User.public_key == public_key).limit(1)
        )
        existing_user_id = res.scalar_one_or_none()

        if existing_user_id is not None:
            LOGGER.warning(f"user_id={existing_user_id}: user already exists")
            raise ValueError("User already exists")

        user = ComponentDB(
            user_id=user_id,
            public_key=public_key,
        )

        self.session.add(user)
        await self.session.commit()
        await self.session.refresh(user)

        return viewUser(user)

    async def update_client(self, client_id: str, version: str = "") -> None:
        if not version:
            LOGGER.warning("cannot update a version with an empty string")
            return

        client: ComponentDB = await self.session.scalar(
            select(ComponentDB).where(ComponentDB.component_id == client_id)
        )
        client.version = version

        await self.session.commit()

        LOGGER.info(f"client_id={client_id}: updated client version to {version}")

    async def client_leave(self, client_id: str) -> None:
        await self.invalidate_all_tokens(client_id)

        client: ComponentDB = await self.session.scalar(
            select(ComponentDB).where(ComponentDB.component_id == client_id)
        )
        client.active = False
        client.left = True

        await self.session.commit()

    async def get_by_id(self, component_id: str) -> ComponentView:
        res = await self.session.execute(
            select(ComponentDB).where(ComponentDB.component_id == component_id)
        )
        return view(res.scalar_one())

    async def get_by_key(self, public_key: str) -> ComponentView:
        res = await self.session.execute(
            select(ComponentDB).where(ComponentDB.public_key == public_key)
        )
        return view(res.scalar_one())

    async def get_by_token(self, token: str) -> ComponentView:
        res = await self.session.execute(
            select(ComponentDB)
            .join(Token, ComponentDB.component_id == Token.token_id)
            .where(Token.token == token)
        )
        return view(res.scalar_one())

    async def list_components(self, component_type: str |None= None) -> list[ComponentView]:

        stmt = select(ComponentDB)
        if component_type is not None:
            stmt = stmt.where(ComponentDB.type == component_type)

        res = await self.session.scalars(stmt)
        return [view(c) for c in res.all()]

    async def list_clients(self):
        return self.list(TYPE_CLIENT)

    async def list_users(self):
        return self.list(TYPE_WORKBENCH)

    async def create_token(self, token: Token) -> Token:
        LOGGER.info(f"component_id={token.component_id}: creating new token")

        res = await self.session.execute(
            select(Token).where(Token.token == token.token)
        )

        existing_token: Token | None = res.scalar_one_or_none()

        if existing_token is not None:
            LOGGER.warning(
                f"component_id={existing_token.component_id}: a valid token already exists"
            )
            # TODO: check if we have more strong condition for this
            return existing_token

        self.session.add(token)
        await self.session.commit()
        await self.session.refresh(token)

        return token

    async def invalidate_tokens(self, component_id: str) -> None:
        res = await self.session.scalars(
            select(Token).where(Token.component_id == component_id)
        )
        tokens: list[Token] = res.all()

        for token in tokens:
            token.valid = False

        await self.session.commit()

    async def get_component_id_by_token(self, token: str) -> str | None:
        res = await self.session.scalars(select(Token).where(Token.token == token))
        client_token: Token | None = res.one_or_none()

        if client_token is None:
            return None

        return str(client_token.component_id)

    async def get_token_by_token(self, token: str) -> Token | None:
        res = await self.session.execute(select(Token).where(Token.token == token))
        return res.scalar_one_or_none()

    async def get_token_by_component_id(self, client_id: str) -> Token | None:
        res = await self.session.execute(
            select(Token).where(Token.component_id == client_id, Token.valid == True)
        )
        return res.scalar_one_or_none()

    async def get_token_by_client_type(self, client_type: str) -> str | None:
        client_token: Token | None = await self.session.scalar(
            select(Token)
            .join(ComponentDB, ComponentDB.component_id == Token.component_id)
            .where(ComponentDB.type == client_type)
            .limit(1)
        )

        if client_token is None:
            return None

        return str(client_token.token)

    async def create_event(self, component_id: str, event: str) -> Event:
        LOGGER.debug(f'component_id={component_id}: creating new event="{event}"')

        session_event = Event(component_id=component_id, event=event)

        self.session.add(session_event)
        await self.session.commit()
        await self.session.refresh(session_event)

        return session_event

    async def get_events(self, component_id: str) -> list[Event]:
        res = await self.session.scalars(
            select(Event).where(Event.component_id == component_id)
        )
        return res.all()

