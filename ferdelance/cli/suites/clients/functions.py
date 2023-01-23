from typing import List

import pandas as pd

from ....database import DataBase
from ....database.schemas import Component
from ....database.services import ComponentService
from ...visualization import show_many, show_one


async def list_clients() -> List[Component]:
    """Print and return Component objects list

    Returns:
        List[Component]: List of Component objects
    """
    db = DataBase()
    async with db.async_session() as session:
        client_service = ComponentService(session)
        clients: List[Component] = await client_service.list_clients()
        show_many(result=clients)
        return clients


async def describe_client(client_id: str) -> Component:
    """Describe single client by printing its db record.

    Args:
        client_id (str): Unique ID of the client

    Raises:
        ValueError: if no client id is provided

    Returns:
        Component: the Component object
    """

    if client_id is None:
        raise ValueError("Provide a Component ID")

    db = DataBase()
    async with db.async_session() as session:

        client_service = ComponentService(session)

        client: Component | None = await client_service.get_client_by_id(client_id)

        if client is None:
            print(f"No client found with id {client_id}")
        else:
            show_one(client)

        return client
