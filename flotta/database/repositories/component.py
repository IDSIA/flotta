from flotta.const import TYPE_CLIENT, TYPE_NODE, TYPE_USER
from flotta.database.tables import (
    Component as ComponentDB,
    ComponentType,
    Event as EventDB,
)
from flotta.database.repositories.core import AsyncSession, Repository
from flotta.logging import get_logger
from flotta.schemas.components import Component, Event

from sqlalchemy import select
from sqlalchemy.exc import NoResultFound


LOGGER = get_logger(__name__)


def viewComponent(component: ComponentDB) -> Component:
    assert component.version is not None
    assert component.ip_address is not None
    assert component.url is not None

    return Component(
        id=component.id,
        type_name=component.type_name,
        name=component.name,
        public_key=component.public_key,
        active=component.active,
        left=component.left,
        version=component.version,
        blacklisted=component.blacklisted,
        ip_address=component.ip_address,
        url=component.url,
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

    This repository has the role of manger only for components, event log, and
    the special cases of clients.
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

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

    async def create_component(
        self,
        component_id: str,
        type_name: str,
        public_key: str,
        version: str,
        name: str,
        ip_address: str,
        url: str,
        is_self: bool = False,
        is_join: bool = False,
        active: bool = True,
        blacklisted: bool = False,
        left: bool = False,
    ) -> Component:
        """Creates a generic component. All components are defined by a public
        key and a type. The type defines which area can be accessed by the
        component, while the public key will help in the communication between
        server and component.

        Args:
            component_id (str):
                Id assigned to the component.
            type_name (str):
                Name of the type of the component. Refer to the
                flotta.const.COMPONENT_TYPE list for all the types.
            public_key (str):
                Public key sent by the remote component and used for encryption.
            name (str):
                Name sent by the client.
            version (str):
                Version of the library used by the client.
            machine_system (str):
                Operative system sent by the client.
            machine_mac_address (str):
                Hardware address sent by the client.
            machine_node (str):
                Unique machine identifier sent by the client.
            ip_address (str):
                Ip address that the client used for the join request.
            url (str):
                Url where the machine can be reach at.
            is_self (bool):
                Flag to determine if the new component identifies the node itself
                or not.

        Raises:
            ValueError:
                If the public key is already used by some other component; or
                if there exists in the database a client with the same MAC
                address, machine node, or public key.

        Returns:
            Component:
                A handler to the component object.
        """

        LOGGER.info(f"component={component_id}: creating new type={type_name} version={version} name={name} url={url}")

        if type_name == TYPE_USER:
            await self._check_for_existing_user(public_key)

        else:
            await self._check_for_existing_component(component_id, public_key)

        component = ComponentDB(
            id=component_id,
            version=version,
            public_key=public_key,
            ip_address=ip_address,
            type_name=type_name,
            name=name,
            is_self=is_self,
            is_join=is_join,
            url=url,
            active=active,
            blacklisted=blacklisted,
            left=left,
        )

        self.session.add(component)

        await self.session.commit()
        await self.session.refresh(component)

        return viewComponent(component)

    async def _check_for_existing_user(self, public_key: str):
        res = await self.session.scalars(select(ComponentDB.id).where((ComponentDB.public_key == public_key)).limit(1))
        existing_id: str | None = res.one_or_none()

        if existing_id is not None:
            LOGGER.warning(f"component={existing_id}: already exists")
            raise ValueError("Component already exists")

    async def _check_for_existing_component(self, component_id: str, public_key: str):
        res = await self.session.scalars(
            select(ComponentDB.id)
            .where((ComponentDB.id == component_id) | (ComponentDB.public_key == public_key))
            .limit(1)
        )

        existing_id: str | None = res.one_or_none()

        if existing_id is not None:
            LOGGER.warning(f"A {TYPE_CLIENT} already exists with component={existing_id}")
            raise ValueError("Component already exists")

    async def update_component(self, component_id: str, version: str) -> None:
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

        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.id == component_id))

        client: ComponentDB = res.one()
        client.version = version

        await self.session.commit()

        LOGGER.info(f"component={component_id}: updated client version to {version}")

    async def component_leave(self, component_id: str) -> None:
        """Marks a client as it left the server and it is no more available
        for jobs or data source analysis. The client will also be marked
        as inactive.

        Args:
            component_id (str):
                Id of the client to mark as left.

        Raises:
            NoResultException:
                If there is no client with the given id.
        """
        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.id == component_id))
        client: ComponentDB = res.one()
        client.active = False
        client.left = True

        await self.session.commit()

    async def get_by_id(self, component_id: str) -> Component:
        """Return a component given its id. Note that if it is a client type,
        it will still be returned as a component. To return a Component handler,
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

    async def get_client_by_id(self, component_id: str) -> Component:
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
        return viewComponent(res.one())

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

    async def list_components(self) -> list[Component]:
        """Lists all components registered in the database.

        Returns:
            list[Component]:
                A list of handlers. Note that it can be an empty list.
        """
        res = await self.session.scalars(select(ComponentDB))
        return [viewComponent(c) for c in res.all()]

    async def list_clients(self) -> list[Component]:
        """Lists all components of client type.

        Returns:
            list[Component]:
                A list of client handlers. Note that this can be an empty list.
        """
        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.type_name == TYPE_CLIENT))
        return [viewComponent(c) for c in res.all()]

    async def list_nodes(self) -> list[Component]:
        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.type_name == TYPE_NODE))
        return [viewComponent(c) for c in res.all()]

    async def list_clients_by_ids(self, client_ids: list[str]) -> list[Component]:
        """Lists all the clients that have the id in the list of ids.

        Args:
            client_ids (list[str]):
                List of client ids.

        Returns:
            list[Component]:
                All the clients found given the input list. Note that this can
                be an empty list.
        """
        res = await self.session.scalars(
            select(ComponentDB).where(
                ComponentDB.id.in_(client_ids),
                ComponentDB.type_name == TYPE_CLIENT,
            )
        )

        return [viewComponent(c) for c in res.all()]

    async def get_self_component(self) -> Component:
        """Returns:
        str:
            The component associated with the node itself.
        """

        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.is_self == True).limit(1))  # noqa: E712

        component: ComponentDB = res.one()

        return viewComponent(component)

    async def get_join_component(self) -> Component:
        """Returns:
        str:
            The component associated with the node itself.
        """

        res = await self.session.scalars(select(ComponentDB).where(ComponentDB.is_join == True).limit(1))  # noqa: E712

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

        LOGGER.debug(f'component={component_id}: creating new event="{event}"')

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
