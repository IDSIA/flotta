from ferdelance.database.tables import DataSource as DataSourceDB, Project as ProjectDB, project_datasource
from ferdelance.database.repositories.core import AsyncSession, Repository
from ferdelance.database.repositories.component import viewClient, ComponentDB, Client
from ferdelance.schemas.metadata import Metadata, MetaDataSource
from ferdelance.schemas.datasources import DataSource, Feature
from ferdelance.config import conf

from sqlalchemy import select
from sqlalchemy.exc import NoResultFound

from datetime import datetime
from uuid import uuid4

import aiofiles
import aiofiles.os as aos
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


def view(datasource: DataSourceDB, features: list[Feature]) -> DataSource:
    return DataSource(
        datasource_id=datasource.datasource_id,
        datasource_hash=datasource.datasource_hash,
        name=datasource.name,
        creation_time=datasource.creation_time,
        update_time=datasource.update_time,
        removed=datasource.removed,
        n_records=datasource.n_records,
        n_features=datasource.n_features,
        client_id=datasource.component_id,
        features=features,
    )


class DataSourceRepository(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_or_update_metadata(self, client_id: str, metadata: Metadata) -> None:
        for ds in metadata.datasources:
            await self.create_or_update_datasource(client_id, ds, False)
        await self.session.commit()

        LOGGER.info(f"client_id={client_id}: added {len(metadata.datasources)} new datasources")

    async def create_or_update_datasource(
        self, client_id: str, meta_ds: MetaDataSource, commit: bool = True
    ) -> DataSource:
        dt_now = datetime.now()

        res = await self.session.execute(
            select(DataSourceDB).where(
                DataSourceDB.component_id == client_id,
                DataSourceDB.datasource_hash == meta_ds.datasource_hash,
            )
        )

        # check if ds exists:
        ds_db: DataSourceDB | None = res.scalar_one_or_none()

        if ds_db is None:
            # create a new data source for this client
            LOGGER.info(f"client_id={client_id}: creating new data source={meta_ds.name}")

            meta_ds.datasource_id = str(uuid4())

            ds = DataSource(**meta_ds.dict(), client_id=client_id)
            path = await self.store(ds)

            ds_db = DataSourceDB(
                datasource_id=ds.datasource_id,
                datasource_hash=ds.datasource_hash,
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

                await self.remove(ds_db.datasource_id)

            else:
                # update data source info
                LOGGER.info(f"client_id={client_id}: updating data source={ds_db.name}")

                meta_ds.datasource_id = ds_db.datasource_id
                ds_db.n_records = meta_ds.n_records
                ds_db.n_features = meta_ds.n_features
                ds_db.update_time = dt_now

                ds = DataSource(**meta_ds.dict(), client_id=client_id)
                path = await self.store(ds)

        if commit:
            await self.session.commit()
            await self.session.refresh(ds_db)

        stored_ds = await self.load(ds_db.datasource_id)

        return view(ds_db, stored_ds.features)

    async def storage_location(self, datasource_id: str) -> str:
        path = conf.storage_dir_datasources(datasource_id)
        await aos.makedirs(path, exist_ok=True)
        return os.path.join(path, "datasource.json")

    async def store(self, datasource: DataSource) -> str:
        """Can raise ValueError."""

        path = await self.storage_location(datasource.datasource_id)

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(datasource.dict())
            await f.write(content)

        return path

    async def load(self, datasource_id: str) -> DataSource:
        """Can raise ValueError."""
        try:
            path = await self.storage_location(datasource_id)

            if not await aos.path.exists(path):
                raise ValueError(f"datasource_id={datasource_id} not on disk")

            async with aiofiles.open(path, "r") as f:
                content = await f.read()
                return DataSource(**json.loads(content))

        except NoResultFound as _:
            raise ValueError(f"datasource_id={datasource_id} not found")

    async def remove(self, datasource_id: str) -> None:
        datasource_path: str = await self.get_datasource_path(datasource_id)

        if not await aos.path.exists(datasource_path):
            raise ValueError(f"datasource_id={datasource_id} not found")

        await aos.remove(datasource_path)

    async def get_datasource_path(self, datasource_id: str) -> str:
        """Can raise NoResultFound"""
        res = await self.session.scalars(select(DataSourceDB.path).where(DataSourceDB.datasource_id == datasource_id))
        return res.one()

    async def get_datasource_list(self) -> list[DataSource]:
        res = await self.session.scalars(select(DataSourceDB))
        return [view(d, list()) for d in res.all()]

    async def get_datasources_by_client_id(self, client_id: str) -> list[DataSource]:
        res = await self.session.scalars(select(DataSourceDB).where(DataSourceDB.component_id == client_id))
        return [view(d, list()) for d in res.all()]

    async def get_hash_by_client_and_project(self, client_id: str, project_id: str) -> list[str]:
        res = await self.session.scalars(
            select(DataSourceDB.datasource_hash)
            .join(project_datasource)
            .join(ProjectDB)
            .where(DataSourceDB.component_id == client_id, ProjectDB.project_id == project_id)
        )

        return list(res.all())

    async def get_datasource_by_id(self, datasource_id: str) -> DataSource:
        """Can raise NoResultsFound."""
        res = await self.session.scalars(
            select(DataSourceDB).where(
                DataSourceDB.datasource_id == datasource_id,
                DataSourceDB.removed == False,
            )
        )
        ds = res.one()
        stored_ds = await self.load(datasource_id)
        return view(ds, stored_ds.features)

    async def get_client_by_datasource_id(self, ds_id: str) -> Client:
        """Can raise NoResultFound."""
        res = await self.session.scalars(
            select(ComponentDB)
            .join(DataSourceDB, ComponentDB.component_id == DataSourceDB.component_id)
            .where(
                DataSourceDB.datasource_id == ds_id,
                DataSourceDB.removed == False,
            )
        )
        return viewClient(res.one())
