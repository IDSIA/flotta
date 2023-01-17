import pandas as pd

from ...database import DataBase
from ...database.schemas import Client
from ...database.services import ClientService


async def list_clients(**kwargs) -> pd.DataFrame:
    """Print Client list, with or without filters on client_ID, client_id"""
    client_id = kwargs.get("client_id", None)

    db = DataBase()
    async with db.async_session() as session:

        cs = ClientService(session)

        if client_id is not None:
            clients_session: list[Client] = [await cs.get_client_by_id(client_id)]
        else:
            clients_session: list[Client] = await cs.get_client_list()

        clients_list = [c.dict() for c in clients_session]

        result: pd.DataFrame = pd.DataFrame(clients_list)

        print(result)

        return result


async def describe_client(client_id: str, **kwargs) -> Client:
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

        cs = ClientService(session)

        client: Client | None = await cs.get_client_by_id(client_id)

        if client is None:
            print(f"No client found with id {client_id}")
        else:
            print(pd.Series(client.dict()))

        return client
