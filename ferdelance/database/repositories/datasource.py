from ferdelance.config import config_manager, get_logger
from ferdelance.database.tables import DataSource as DataSourceDB, Project as ProjectDB, project_datasource
from ferdelance.database.repositories.core import AsyncSession, Repository
from ferdelance.database.repositories.component import viewClient, ComponentDB, Client
from ferdelance.schemas.metadata import Metadata, MetaDataSource
from ferdelance.schemas.datasources import DataSource, Feature

from sqlalchemy import select
from sqlalchemy.exc import NoResultFound

from datetime import datetime
from uuid import uuid4

import aiofiles
import aiofiles.os as aos
import json
import os

LOGGER = get_logger(__name__)


def view(datasource: DataSourceDB, features: list[Feature]) -> DataSource:
    return DataSource(
        id=datasource.id,
        hash=datasource.hash,
        name=datasource.name,
        creation_time=datasource.creation_time,
        update_time=datasource.update_time,
        removed=datasource.removed,
        n_records=datasource.n_records,
        n_features=datasource.n_features,
        component_id=datasource.component_id,
        features=features,
    )


class DataSourceRepository(Repository):
    """A repository for data sources.

    A datasource is a description of the data collected and managed by a component
    (a client in current state). This information is limited to the descriptions
    of the data, also called "metadata".

    This repository is an hybrid repository since part of the data are stored
    locally on disk. The database contains information on timings and path
    to the real data sources on disk.
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_or_update_from_metadata(self, client_id: str, metadata: Metadata) -> None:
        """Creates or updates records on datasources based on the metadata received by the client.

        Args:
            client_id (str):
                Id of the client that sent the metadata.
            metadata (Metadata):
                Metadata content sent by the client.
        """

        for ds in metadata.datasources:
            await self.create_or_update_datasource(client_id, ds, False)
        await self.session.commit()

        LOGGER.info(f"client_id={client_id}: added {len(metadata.datasources)} new datasources")

    async def create_or_update_datasource(
        self, client_id: str, meta_ds: MetaDataSource, commit: bool = True
    ) -> DataSource:
        """Creates or updates records on datasources based on the received data.
        If the datasource, identified by the hash created by the client, already
        exists, then it will be updated; otherwise a new datasource will be
        created.

        Whit this update, a datasource can also be removed, if marked for
        elimination.

        Args:
            client_id (str):
                Id of the client that sent the metadata.
            meta_ds (MetaDataSource):
                Data on the datasources present in the metadata sent by the client.
            commit (bool, optional):
                If set to False, the transaction to the database will not be
                committed. Use this flag if you plan to edit multiple datasource
                at once.
                Defaults to True.

        Returns:
            DataSource:
                An handler to the edited data source.
        """
        dt_now = datetime.now()

        res = await self.session.execute(
            select(DataSourceDB).where(
                DataSourceDB.component_id == client_id,
                DataSourceDB.hash == meta_ds.hash,
            )
        )

        # check if ds exists:
        ds_db: DataSourceDB | None = res.scalar_one_or_none()

        if ds_db is None:
            # create a new data source for this client
            LOGGER.info(f"client_id={client_id}: creating new data source={meta_ds.name}")

            meta_ds.id = str(uuid4())

            ds = DataSource(**meta_ds.dict(), component_id=client_id)
            path = await self.store(ds)

            ds_db = DataSourceDB(
                id=ds.id,
                hash=ds.hash,
                name=ds.name,
                path=path,
                n_records=ds.n_records,
                n_features=ds.n_features,
                component_id=client_id,
            )

            self.session.add(ds_db)

        else:
            if meta_ds.removed:
                # remove data source info and from disk, keep placeholder
                LOGGER.info(f"client_id={client_id}: removing data source={ds_db.name}")

                ds_db.removed = True
                ds_db.n_records = None
                ds_db.n_features = None
                ds_db.update_time = dt_now

                await self.remove(ds_db.id)

            else:
                # update data source info
                LOGGER.info(f"client_id={client_id}: updating data source={ds_db.name}")

                meta_ds.id = ds_db.id
                ds_db.n_records = meta_ds.n_records
                ds_db.n_features = meta_ds.n_features
                ds_db.update_time = dt_now

                ds = DataSource(**meta_ds.dict(), component_id=client_id)
                path = await self.store(ds)

        if commit:
            await self.session.commit()
            await self.session.refresh(ds_db)

        stored_ds = await self.load(ds_db.id)

        return view(ds_db, stored_ds.features)

    async def storage_location(self, datasource_id: str) -> str:
        """Checks that the output directory for this datasource exists. If not
        it will be created. Then it creates a path for the destination file.

        The datasource file is supposed to be stored in the JSON format.

        Args:
            datasource_id (str):
                Id of the datasource to save to or get from disk.

        Returns:
            str:
                A valid path to the directory where the datasource can be saved
                to or loaded from. Path is considered to be a JSON file.
        """
        path = config_manager.get().storage_datasources(datasource_id)
        await aos.makedirs(path, exist_ok=True)
        return os.path.join(path, "datasource.json")

    async def store(self, datasource: DataSource) -> str:
        """Save a datasource on disk in JSON format.

        Args:
            datasource (DataSource):
                The datasource content that will be saved on disk.

        Returns:
            str:
                The path where the data have been saved to.
        """
        path = await self.storage_location(datasource.id)

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(datasource.dict())
            await f.write(content)

        return path

    async def load(self, datasource_id: str) -> DataSource:
        """Load a datasource from disk given its id, if it has been found on
        disk and in the database.

        Args:
            datasource_id (str):
                Id of the datasource to load.

        Raises:
            ValueError:
                If the datasource path has not been found on disk.
            ValueError:
                If the datasource_id was not found in the database.

        Returns:
            DataSource:
                The requested datasource.
        """

        try:
            path = await self.storage_location(datasource_id)

            if not await aos.path.exists(path):
                raise ValueError(f"datasource_id={datasource_id} not on disk")

            async with aiofiles.open(path, "r") as f:
                content = await f.read()
                return DataSource(**json.loads(content))

        except NoResultFound:
            raise ValueError(f"datasource_id={datasource_id} not found")

    async def remove(self, datasource_id: str) -> None:
        """Removes a datasource from the disk given its datasource_id, if it exists.

        Args:
            datasource_id (str):
                Id of the datasource to remove.

        Raises:
            ValueError:
                If the datasource does not exists on disk.
        """
        datasource_path: str = await self.get_datasource_path(datasource_id)

        if not await aos.path.exists(datasource_path):
            raise ValueError(f"datasource_id={datasource_id} not found")

        await aos.remove(datasource_path)

    async def get_datasource_path(self, datasource_id: str) -> str:
        """Return the location, or path, where the datasource is stored on disk,
        if it exists in the database.

        Args:
            datasource_id (str):
                Id of the datasource to search for.

        Raises:
            NoResultFound:
                If the given datasource_id has not been found in the database.

        Returns:
            str:
                The path where the datasource is stored on disk.
        """
        res = await self.session.scalars(select(DataSourceDB.path).where(DataSourceDB.id == datasource_id))
        return res.one()

    async def list_datasources(self) -> list[DataSource]:
        """List all datasources stored in the database.

        Returns:
            list[DataSource]:
                A list with all the datasources available. Note that this list
                can be an empty list.
        """
        res = await self.session.scalars(select(DataSourceDB))
        return [view(d, list()) for d in res.all()]

    async def list_datasources_by_client_id(self, client_id: str) -> list[DataSource]:
        """List all the datasources that have been sent to the server by the
        given client_id.

        Args:
            client_id (str):
                Id of the client that has the requested data sources.

        Returns:
            list[DataSource]:
                A list with all the datasources of the client. Note that this
                list can be an empty list.
        """
        res = await self.session.scalars(select(DataSourceDB).where(DataSourceDB.component_id == client_id))
        return [view(d, list()) for d in res.all()]

    async def list_hash_by_client_and_project(self, client_id: str, project_id: str) -> list[str]:
        """List all the hashes of the data sources assigned to a given project
        and sent by a specific client_id.

        Args:
            client_id (str):
                Id of the client that has sent the data sources.
            project_id (str):
                Id of the project that the data source is part of.

        Returns:
            list[str]:
                A list of hashes. Note that this can be an empty list().
        """
        res = await self.session.scalars(
            select(DataSourceDB.hash)
            .join(project_datasource)
            .join(ProjectDB)
            .where(DataSourceDB.component_id == client_id, ProjectDB.id == project_id)
        )

        return list(res.all())

    async def get_datasource_by_id(self, datasource_id: str) -> DataSource:
        """Returns a data source, given its datasource_id. The data source
        includes the data stored on disk.

        Args:
            datasource_id (str):
                The id of the datasource to search for.

        Raises:
            NoResultsFound:
                If the datasource_id has not been found in the database.

        Returns:
            DataSource:
                The descriptor found in the database with all the data stored
                on disk.
        """
        res = await self.session.scalars(
            select(DataSourceDB).where(
                DataSourceDB.id == datasource_id,
                DataSourceDB.removed == False,  # noqa: E712
            )
        )
        ds = res.one()
        stored_ds = await self.load(datasource_id)
        return view(ds, stored_ds.features)

    async def get_client_by_datasource_id(self, datasource_id: str) -> Client:
        """Return information on the client of the given datasource_id.

        Args:
            datasource_id (str):
                Id of the datasource to search for.

        Raises:
            NoResultsFound:
                If the datasource_id has not been found in the database.

        Returns:
            Client:
                The client that has sent the data source data, if found.
        """
        res = await self.session.scalars(
            select(ComponentDB)
            .join(DataSourceDB, ComponentDB.id == DataSourceDB.component_id)
            .where(
                DataSourceDB.id == datasource_id,
                DataSourceDB.removed == False,  # noqa: E712
            )
        )
        return viewClient(res.one())
