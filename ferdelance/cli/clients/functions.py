from typing import List

import pandas as pd

from ...database import DataBase
from ...database.schemas import Client
from ...database.services import ClientService
from ..visualization import show_many, show_one


async def list_clients() -> List[Client]:
    """Print and return Client objects list

    Returns:
        List[Client]: List of Client objects
    """
    db = DataBase()
    async with db.async_session() as session:
        client_service = ClientService(session)
        clients: List[Client] = await client_service.get_client_list()
        show_many(result=clients)
        return clients


async def describe_client(client_id: str) -> Client:
    """Describe single client by printing its db record.

    Args:
        client_id (str): Unique ID of the client

    Raises:
        ValueError: if no client id is provided

    Returns:
        Client: the Client object
    """

    if client_id is None:
        raise ValueError("Provide a Client ID")

    db = DataBase()
    async with db.async_session() as session:

        client_service = ClientService(session)

        client: Client | None = await client_service.get_client_by_id(client_id)

        if client is None:
            print(f"No client found with id {client_id}")
        else:
            show_one(client)

        return client
