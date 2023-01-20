from typing import List

from ferdelance.cli.visualization import show_many, show_one
from ferdelance.database import DataBase
from ferdelance.database.schemas import ClientDataSource
from ferdelance.database.services import DataSourceService


async def list_datasources(client_id: str = None) -> List[ClientDataSource]:
    """Print and Return DataSource objects list

    Args:
        client_id (str, optional): Filter by client id. Defaults to None.

    Returns:
        List[DataSource]: List of DataSource objects
    """
    db = DataBase()
    async with db.async_session() as session:
        datasource_service: DataSourceService = DataSourceService(session)
        # query_function: Callable = (
        #     partial(datasource_service.get_datasource_by_client_id, client_id=client_id)
        #     if client_id is not None
        #     else datasource_service.get_datasource_list()
        # )
        if client_id is None:
            datasources: List[ClientDataSource] = await datasource_service.get_datasource_list()
        else:
            datasources: List[ClientDataSource] = await datasource_service.get_datasource_by_client_id(
                client_id=client_id
            )

        show_many(datasources)
        return datasources


async def describe_datasource(datasource_id: str) -> ClientDataSource | None:
    """Print and return a single Artifact object

    Args:
        artifact_id (str, optional): Which datasource to describe.

    Raises:
        ValueError: if no datasource id is provided

    Returns:
        Artifact: The Artifact object
    """
    if datasource_id is None:
        raise ValueError("Provide a DataSource ID")

    db = DataBase()
    async with db.async_session() as session:
        datasource_service: DataSourceService = DataSourceService(session)
        datasource: ClientDataSource | None = await datasource_service.get_datasource_by_id(ds_id=datasource_id)
        if datasource is None:
            print(f"No Datasource found with id {datasource_id}")
        else:
            show_one(datasource)
        return datasource
