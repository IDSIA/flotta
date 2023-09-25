from ferdelance.database import DataBase
from ferdelance.schemas.components import Component
from ferdelance.database.repositories import ComponentRepository
from ferdelance.cli.visualization import show_many, show_one

from sqlalchemy.exc import NoResultFound


async def list_clients() -> list[Component]:
    """Print and return Component objects list

    Returns:
        List[Component]: List of Component objects
    """
    db = DataBase()
    async with db.async_session() as session:
        component_repository = ComponentRepository(session)
        clients: list[Component] = await component_repository.list_clients()
        show_many(clients)
        return clients


async def describe_client(client_id: str) -> Component | None:
    """Describe single client by printing its db record.

    Args:
        client_id (str): Unique ID of the client

    Raises:
        ValueError: if no client id is provided

    Returns:
        Component: the Component object
    """

    if client_id is None:
        raise ValueError("Provide a Client ID")

    db = DataBase()
    async with db.async_session() as session:
        component_repository = ComponentRepository(session)

        try:
            client: Component = await component_repository.get_client_by_id(client_id)
            show_one(client)
            return client
        except NoResultFound:
            print(f"No client found with id {client_id}")
