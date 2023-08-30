from ferdelance.config import get_logger
from ferdelance.database.tables import (
    Component as ComponentDB,
    ComponentType,
    Event as EventDB,
    Token as TokenDB,
)
from ferdelance.database.repositories.core import AsyncSession, Repository
from ferdelance.database.repositories.tokens import TokenRepository
from ferdelance.database.data import TYPE_CLIENT, TYPE_NODE
from ferdelance.schemas.components import (
    Component,
    Client,
    Token,
    Event,
)

from sqlalchemy import select
from sqlalchemy.exc import NoResultFound


LOGGER = get_logger(__name__)


def viewComponent(component: ComponentDB) -> Component:
    return Component(
        id=component.id,
        public_key=component.public_key,
        active=component.active,
        left=component.left,
        type_name=component.type_name,
        name=component.name,
    )


def viewClient(component: ComponentDB) -> Client:
    assert component.version is not None
    assert component.machine_system is not None
    assert component.machine_mac_address is not None
    assert component.machine_node is not None
    assert component.ip_address is not None

    return Client(
        id=component.id,
        public_key=component.public_key,
        active=component.active,
        left=component.left,
        version=component.version,
        machine_system=component.machine_system,
        machine_mac_address=component.machine_mac_address,
        machine_node=component.machine_node,
        blacklisted=component.blacklisted,
        ip_address=component.ip_address,
        type_name=component.type_name,
    )


def viewToken(token: TokenDB) -> Token:
    return Token(
        id=token.id,
        component_id=token.component_id,
        token=token.token,
        creation_time=token.creation_time,
        expiration_time=token.expiration_time,
        valid=token.valid,
    )


def viewEvent(event: EventDB) -> Event:
    return Event(
        component_id=event.component_id,
        id=event.id,
        time=event.time,
        event=event.event,
    )


class ComponentRepository(Repository):
    """A repository used to manage components.

    This repository has the role of manger only for components, but also for
    their access tokens, event log, and the special cases of clients.
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.tr: TokenRepository = TokenRepository(session)

    async def create_types(self, type_names: list[str]):
        """Utility method used to populate the database.

        Args:
            type_names (list[str]):
                List of names of types to be inserted in the database.
        """
        dirty: bool = False

        for type_name in type_names:
            try:
                res = await self.session.scalars(select(ComponentType).where(ComponentType.type == type_name).limit(1))
                res.one()
            except NoResultFound:
                tn = ComponentType(type=type_name)
                self.session.add(tn)
                dirty = True

        if dirty:
            await self.session.commit()

    async def create_client(
        self,
        name: str,
        version: str,
        public_key: str,
        machine_system: str,
        machine_mac_address: str,
        machine_node: str,
        ip_address: str,
    ) -> tuple[Client, Token]:
        """Creates a component of CLIENT type. Arguments are all data sent by
        the remote client to the server. The return is both the client handler
        and the token assigned to the client.

        This method will first create the access token for the client and then
        will populate the database with the information from the client.

        Args:
            name (str):
                Name sent by the client.
            version (str):
                Version of the library used by teh client.
            public_key (str):
                Public key sent by the client.
            machine_system (str):
                Operative system sent by the client.
            machine_mac_address (str):
                Hardware address sent by the client.
            machine_node (str):
                Unique machine identifier sent by the client.
            ip_address (str):
                Ip address that the client used for the join request.

        Raises:
            ValueError:
                If there exists in the database a client with the same MAC
                address, machine node, or public key.

        Returns:
            tuple[Client, Token]:
                A tuple composed by two parts. The first part is the handler to
                the client object; the second part is the handler to the token
                object, created with the client.
        """

        LOGGER.info(
            f"creating new type={TYPE_CLIENT} version={version} "
            f"mac_address={machine_mac_address} node={machine_node} name={name}"
        )

        res = await self.session.scalars(
            select(ComponentDB.id)
            .where(
                (ComponentDB.machine_mac_address == machine_mac_address)
                | (ComponentDB.machine_node == machine_node)
                | (ComponentDB.public_key == public_key)
            )
            .limit(1)
        )

        existing_id: str | None = res.one_or_none()

        if existing_id is not None:
            LOGGER.warning(f"A {TYPE_CLIENT} already exists with component_id={existing_id}")
            raise ValueError("Client already exists")

        token: TokenDB = await self.tr.generate_client_token(
            system=machine_system,
            mac_address=machine_mac_address,
            node=machine_node,
        )
        self.session.add(token)

        component = ComponentDB(
            id=token.component_id,
            version=version,
            public_key=public_key,
            machine_system=machine_system,
            machine_mac_address=machine_mac_address,
            machine_node=machine_node,
            ip_address=ip_address,
            type_name=TYPE_CLIENT,
            name=name,
        )

        self.session.add(component)

        await self.session.commit()
        await self.session.refresh(component)
        await self.session.refresh(token)

        return viewClient(component), viewToken(token)

    async def create_component(self, type_name: str, public_key: str, name: str) -> tuple[Component, Token]:
        """Creates a generic component. All components are defined by a public
        key and a type. The type defines which area can be accessed by the
        component, while the public key will help in the communication between
        server and component.

        Args:
            type_name (str):
                Name of the type of the component. Refer to the
                ferdelance.database.data.COMPONENT_TYPE list for all the types.
            public_key (str):
                Public key sent by the remote component and used for encryption.

        Raises:
            ValueError:
                If the public key is already used by some other component.

        Returns:
            tuple[Component, Token]:
                A tuple composed by two parts. The first part is the handler to
                the component object; the second part is the handler to the token
                object, created for the component.
        """

        LOGGER.info(f"creating new component type={type_name}")

        res = await self.session.scalars(select(ComponentDB.id).where(ComponentDB.public_key == public_key).limit(1))
        existing_user_id: str | None = res.one_or_none()

        if existing_user_id is not None:
            LOGGER.warning(f"user_id={existing_user_id}: user already exists")
            raise ValueError("User already exists")

        token: TokenDB = await self.tr.generate_user_token()
        self.session.add(token)

        component = ComponentDB(
            id=token.component_id,
            public_key=public_key,
            type_name=type_name,
            name=name,
        )

        self.session.add(component)

        await self.session.commit()
        await self.session.refresh(component)
        await self.session.refresh(token)

        return viewComponent(component), viewToken(token)

    async def has_invalid_token(self, component_id: str) -> bool:
        """Wrapper of TokenRepository#count_valid_tokens(), returns True if there
        is no valid token for the component_id; otherwise False.

        Args:
            component_id (str):
                Id of the component to test for

        Returns:
            bool:
                True is there is zero valid tokens, otherwise False.
        """
        n_tokens: int = await self.tr.count_valid_tokens(component_id)

        LOGGER.debug(f"client_id={component_id}: found {n_tokens} valid token(s)")

        return n_tokens == 0

    async def update_client(self, client_id: str, version: str) -> None:
        """Update the version used by the given client to the new value.

        Args:
            client_id (str):
                Id of the client to update.
            version (str):
                New version in use by the client.

        Raises:
            NoResultException:
                If there is no client with the given id.
        """
        if not version:
            LOGGER.warning("cannot update a version with an empty string")
            return

        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.id == client_id))

        client: ComponentDB = res.one()
        client.version = version

        await self.session.commit()

        LOGGER.info(f"client_id={client_id}: updated client version to {version}")

    async def client_leave(self, client_id: str) -> None:
        """Marks a client as it left the server and it is no more available
        for jobs or data source analysis. The client will also be marked
        as inactive.

        All access tokens will be invalidated with this action.

        Args:
            client_id (str):
                Id of the client to mark as left.

        Raises:
            NoResultException:
                If there is no client with the given id.
        """
        await self.invalidate_tokens(client_id)

        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.id == client_id))
        client: ComponentDB = res.one()
        client.active = False
        client.left = True

        await self.session.commit()

    async def get_by_id(self, component_id: str) -> Component:
        """Return a component given its id. Note that if it is a client type,
        it will still be returned as a component. To return a Client handler,
        use the #get_client_by_id() method.

        Args:
            component_id (str):
                Id of the component to get.

        Raises:
            NoResultFound:
                If there is no component with the given id.

        Returns:
            Component:
                The component associated with the given component_id.
        """
        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.id == component_id))
        o: ComponentDB = res.one()
        return viewComponent(o)

    async def get_client_by_id(self, component_id: str) -> Client:
        """Return a component of client type given its id. Note that the component
        must be of TYPE_CLIENT, otherwise it will not return anything.

        Args:
            component_id (str):
                Id of the client to get.

        Raises:
            NoResultFound:
                If there is no client with the given id.

        Returns:
            Component:
                The client associated with the given client_id.
        """
        res = await self.session.scalars(
            select(ComponentDB).where(ComponentDB.id == component_id, ComponentDB.type_name == TYPE_CLIENT)
        )
        return viewClient(res.one())

    async def get_by_key(self, public_key: str) -> Component:
        """Return a component given its public key.

        Args:
            public_key (str):
                Public key sent by the component.

        Raises:
            NoResultFound:
                If there is no component with the given key.

        Returns:
            Component:
                The component associated with the given public_key.
        """
        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.public_key == public_key))

        component: ComponentDB = res.one()

        return viewComponent(component)

    async def get_by_token(self, token: str) -> Component:
        """Return a component given its access token.

        Args:
            token (str):
                Access token used by the component.

        Raises:
            NoResultFound:
                If there is no component with the given token.

        Returns:
            Component:
                The component associated with the given token.
        """
        res = await self.session.scalars(
            select(ComponentDB).join(TokenDB, ComponentDB.id == TokenDB.id).where(TokenDB.token == token)
        )

        component: ComponentDB = res.one()

        return viewComponent(component)

    async def list_components(self) -> list[Component]:
        """Lists all components registered in the database.

        Returns:
            list[Component]:
                A list of handlers. Note that it can be an empty list.
        """
        res = await self.session.scalars(select(ComponentDB))
        return [viewComponent(c) for c in res.all()]

    async def list_clients(self) -> list[Client]:
        """Lists all components of client type.

        Returns:
            list[Client]:
                A list of client handlers. Note that this can be an empty list.
        """
        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.type_name == TYPE_CLIENT))
        return [viewClient(c) for c in res.all()]

    async def list_nodes(self) -> list[Component]:
        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.type_name == TYPE_NODE))
        return [viewComponent(c) for c in res.all()]

    async def list_clients_by_ids(self, client_ids: list[str]) -> list[Client]:
        """Lists all the clients that have the id in the list of ids.

        Args:
            client_ids (list[str]):
                List of client ids.

        Returns:
            list[Client]:
                All the clients found given the input list. Note that this can
                be an empty list.
        """
        res = await self.session.scalars(
            select(ComponentDB).where(
                ComponentDB.id.in_(client_ids),
                ComponentDB.type_name == TYPE_CLIENT,
            )
        )

        return [viewClient(c) for c in res.all()]

    async def invalidate_tokens(self, component_id: str) -> None:
        """Wrapper of the ferdelance.database.repository.tokens.TokenRepository#invalidate_tokens()
        method.

        Args:
            component_id (str):
                Id of the component that need to be invalidated.
        """
        await self.tr.invalidate_tokens(component_id)

    async def update_client_token(self, client: Client) -> Token:
        """Wrapper of the ferdelance.database.repository.tokens.TokenRepository#update_client_token()
        method.

        Args:
            client (Client):
                Handler of the client, with all the required client data.

        Returns:
            Token:
                The new token to use.
        """
        token = await self.tr.update_client_token(
            client.machine_system,
            client.machine_mac_address,
            client.machine_node,
            client.id,
        )
        return viewToken(token)

    async def get_component_id_by_token(self, token: str) -> str:
        """Returns the component id associated with the given access token.

        Args:
            token (str):
                Access token used by the component.

        Returns:
            str:
                Id of the component.
        """
        res = await self.session.scalars(select(TokenDB).where(TokenDB.token == token))
        token_db: TokenDB = res.one()

        return str(token_db.component_id)

    async def get_token_by_token(self, token: str) -> Token:
        """Returns a Token handler, given the token access string.

        Args:
            token (str):
                Access token used by a component.

        Raises:
            NoResultFound:
                If there is no component or token with the given token.

        Returns:
            Token:
                The handler of the given string.
        """
        res = await self.session.scalars(select(TokenDB).where(TokenDB.token == token))
        return viewToken(res.one())

    async def get_token_by_component_id(self, component_id: str) -> Token:
        """Returns a Token handler, given the token access string.

        Args:
            token (str):
                Access token used by a component.

        Raises:
            NoResultFound:
                If there is no component or token with the given token.

        Returns:
            Token:
                The handler of the given string.
        """
        res = await self.session.scalars(
            select(TokenDB).where(TokenDB.component_id == component_id, TokenDB.valid == True)
        )
        return viewToken(res.one())

    async def get_token_for_self(self) -> str:
        """Return the tokens used by the internal server's workers. This should
        be an unique token for all workers.

        Returns:
            str:
                The token used by the internal workers.
        """

        res = await self.session.scalars(
            select(TokenDB)
            .join(ComponentDB, ComponentDB.id == TokenDB.component_id)
            .where(ComponentDB.name == "localhost")
            .limit(1)
        )

        client_token: TokenDB = res.one()

        return str(client_token.token)

    async def get_self_component(self) -> Component:
        """Return the tokens used by the internal server's workers. This should
        be an unique token for all workers.

        Returns:
            str:
                The token used by the internal workers.
        """

        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.name == "localhost").limit(1))

        component: ComponentDB = res.one()

        return viewComponent(component)

    async def create_event(self, component_id: str, event: str) -> Event:
        """Create an entry in the event log. This is just a simple log.

        Args:
            component_id (str):
                Id of the component that created the event.
            event (str):
                Text describing the event.

        Returns:
            Event:
                Just an event handler.
        """

        LOGGER.debug(f'component_id={component_id}: creating new event="{event}"')

        event_db = EventDB(component_id=component_id, event=event)

        self.session.add(event_db)
        await self.session.commit()
        await self.session.refresh(event_db)

        return viewEvent(event_db)

    async def list_events(self, component_id: str) -> list[Event]:
        """Returns a list of all the events created by a component, identified
        by its id.

        Args:
            component_id (str):
                Id of the component that created the events.

        Returns:
            list[Event]:
                A list of all the events created by the component. Note that
                this can be an empty list.
        """
        res = await self.session.scalars(select(EventDB).where(EventDB.component_id == component_id))
        return [viewEvent(e) for e in res.all()]
