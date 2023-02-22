from ferdelance.cli.visualization import show_many, show_one
from ferdelance.database import DataBase
from ferdelance.schemas.datasources import DataSource
from ferdelance.database.repositories import DataSourceService

from sqlalchemy.exc import NoResultFound


async def list_datasources(client_id: str | None = None) -> list[DataSource]:
    """Print and Return DataSource objects list

    Args:
        component_id (str, optional): Filter by client id. Defaults to None.

    Returns:
        List[DataSource]: List of DataSource objects
    """
    db = DataBase()
    async with db.async_session() as session:
        datasource_service: DataSourceService = DataSourceService(session)
        if client_id is None:
            datasources: list[DataSource] = await datasource_service.get_datasource_list()
        else:
            datasources: list[DataSource] = await datasource_service.get_datasources_by_client_id(client_id=client_id)

        show_many(datasources)
        return datasources


async def describe_datasource(datasource_id: str | None) -> DataSource | None:
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
        try:
            datasource: DataSource = await datasource_service.get_datasource_by_id(datasource_id=datasource_id)
            show_one(datasource)
            return datasource
        except NoResultFound as e:
            print(f"No Datasource found with id {datasource_id}")
