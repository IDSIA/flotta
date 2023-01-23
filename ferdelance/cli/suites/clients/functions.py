from typing import List

import pandas as pd

from ferdelance.database import DataBase
from ferdelance.database.schemas import Component, Client
from ferdelance.database.services import ComponentService
from ferdelance.cli.visualization import show_many, show_one

from sqlalchemy.exc import NoResultFound


async def list_clients() -> List[Client]:
    """Print and return Component objects list

    Returns:
        List[Component]: List of Component objects
    """
    db = DataBase()
    async with db.async_session() as session:
        client_service = ComponentService(session)
        clients: List[Client] = await client_service.list_clients()
        show_many(result=clients)
        return clients


async def describe_client(client_id: str) -> Client:
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

        try:
            client: Client = await client_service.get_client_by_id(client_id)
            show_one(client)
            return client
        except NoResultFound as e:
            print(f"No client found with id {client_id}")
            raise e
