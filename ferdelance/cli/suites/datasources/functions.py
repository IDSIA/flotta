from typing import List

from ferdelance.cli.visualization import show_many, show_one
from ferdelance.database import DataBase
from ferdelance.database.schemas import DataSource
from ferdelance.database.services import DataSourceService


async def list_datasources(component_id: str = None) -> List[DataSource]:
    """Print and Return DataSource objects list

    Args:
        component_id (str, optional): Filter by client id. Defaults to None.

    Returns:
        List[DataSource]: List of DataSource objects
    """
    db = DataBase()
    async with db.async_session() as session:
        datasource_service: DataSourceService = DataSourceService(session)
        # query_function: Callable = (
        #     partial(datasource_service.get_datasource_by_client_id, component_id=component_id)
        #     if component_id is not None
        #     else datasource_service.get_datasource_list()
        # )
        if component_id is None:
            datasources: List[DataSource] = await datasource_service.get_datasource_list()
        else:
            datasources: List[DataSource] = await datasource_service.get_datasource_by_client_id(
                component_id=component_id
            )

        show_many(datasources)
        return datasources


async def describe_datasource(datasource_id: str) -> DataSource | None:
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
        datasource: DataSource | None = await datasource_service.get_datasource_by_id(ds_id=datasource_id)
        if datasource is None:
            print(f"No Datasource found with id {datasource_id}")
        else:
            show_one(datasource)
        return datasource
