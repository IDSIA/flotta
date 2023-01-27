from ferdelance.database.tables import DataSource, Feature
from ferdelance.database.services.component import viewClient, ComponentDB, Client
from ferdelance.database.services.core import AsyncSession, DBSessionService
from ferdelance.database.schemas import DataSource as DataSourceView
from ferdelance.shared.artifacts import Metadata, MetaDataSource, MetaFeature

from datetime import datetime
from uuid import uuid4

from sqlalchemy import select

import logging

LOGGER = logging.getLogger(__name__)


def view(datasource: DataSource) -> DataSourceView:
    return DataSourceView(
        datasource_id=datasource.datasource_id,
        name=datasource.name,
        creation_time=datasource.creation_time,
        update_time=datasource.update_time,
        removed=datasource.removed,
        n_records=datasource.n_records,
        n_features=datasource.n_features,
        client_id=datasource.component_id,
    )


class DataSourceService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_or_update_metadata(self, client_id: str, metadata: Metadata) -> None:
        for ds in metadata.datasources:
            await self.create_or_update_datasource(client_id, ds)

    async def create_or_update_datasource(self, client_id: str, ds: MetaDataSource) -> DataSource:
        dt_now = datetime.now()

        res = await self.session.execute(
            select(DataSource).where(
                DataSource.component_id == client_id,
                DataSource.name == ds.name,
            )
        )

        # check if ds exists:
        ds_db: DataSource | None = res.scalar_one_or_none()

        if ds_db is None:
            # create a new data source for this client
            LOGGER.info(f"client_id={client_id}: creating new data source={ds.name}")

            ds_db = DataSource(
                datasource_id=str(uuid4()),
                name=ds.name,
                n_records=ds.n_records,
                n_features=ds.n_features,
                component_id=client_id,
            )

            self.session.add(ds_db)

        else:
            if ds.removed:
                # remove data source and info
                LOGGER.info(f"client_id={client_id}: removing data source={ds.name}")

                ds_db.removed = True
                ds_db.n_records = None
                ds_db.n_features = None
                ds_db.update_time = dt_now

                # remove features assigned with this data source
                x = await self.session.execute(select(Feature).where(Feature.datasource_id == ds_db.component_id))

                features: list[Feature] = list(x.scalars().all())

                for f in features:
                    f.removed = True
                    f.dtype = None
                    f.v_mean = None
                    f.v_std = None
                    f.v_min = None
                    f.v_p25 = None
                    f.v_p50 = None
                    f.v_p75 = None
                    f.v_max = None
                    f.v_miss = None
                    f.update_time = dt_now

            else:
                # update data source info
                LOGGER.info(f"client_id={client_id}: updating data source={ds.name}")

                ds_db.n_records = ds.n_records
                ds_db.n_features = ds.n_features
                ds_db.update_time = dt_now

        await self.session.commit()
        await self.session.refresh(ds_db)

        for f in ds.features:
            await self.create_or_update_feature(ds_db, f, ds.removed, commit=False)

        await self.session.commit()

        return ds_db

    async def create_or_update_feature(
        self, ds: DataSource, f: MetaFeature, remove: bool = False, commit: bool = True
    ) -> Feature:
        dt_now = datetime.now()

        res = await self.session.execute(
            select(Feature).where(
                Feature.datasource_id == ds.datasource_id,
                Feature.name == f.name,
            )
        )

        f_db: Feature | None = res.scalar_one_or_none()

        if f_db is None:
            LOGGER.info(f"datasource_id={ds.datasource_id}: creating new feature={f.name}")

            f_db = Feature(
                feature_id=str(uuid4()),
                name=f.name,
                dtype=f.dtype,
                v_mean=f.v_mean,
                v_std=f.v_std,
                v_min=f.v_min,
                v_p25=f.v_p25,
                v_p50=f.v_p50,
                v_p75=f.v_p75,
                v_miss=f.v_miss,
                v_max=f.v_max,
                removed=remove,
                datasource_id=ds.datasource_id,
                datasource_name=ds.name,
            )

            self.session.add(f_db)
        else:
            if remove or f.removed:
                # remove feature and info
                LOGGER.info(f"removing feature={f.name} for datasource={ds.datasource_id}")

                f_db.removed = True
                f_db.dtype = None
                f_db.v_mean = None
                f_db.v_std = None
                f_db.v_min = None
                f_db.v_p25 = None
                f_db.v_p50 = None
                f_db.v_p75 = None
                f_db.v_max = None
                f_db.v_miss = None
                f_db.update_time = dt_now

            else:
                # update data source info
                LOGGER.info(f"client_id={ds.datasource_id}: updating data source={f.name}")

                f_db.dtype = f.dtype
                f_db.v_mean = f.v_mean
                f_db.v_std = f.v_std
                f_db.v_min = f.v_min
                f_db.v_p25 = f.v_p25
                f_db.v_p50 = f.v_p50
                f_db.v_p75 = f.v_p75
                f_db.v_max = f.v_max
                f_db.v_miss = f.v_miss
                f_db.update_time = dt_now

        if commit:
            await self.session.commit()
            await self.session.refresh(f_db)

        return f_db

    async def get_datasource_list(self) -> list[DataSourceView]:
        res = await self.session.scalars(select(DataSource))
        return [view(d) for d in res.all()]

    async def get_datasource_by_client_id(self, client_id: str) -> list[DataSourceView]:
        res = await self.session.scalars(select(DataSource).where(DataSource.component_id == client_id))
        return [view(d) for d in res.all()]

    async def get_datasource_ids_by_client_id(self, client_id: str) -> list[str]:
        res = await self.session.scalars(select(DataSource.datasource_id).where(DataSource.component_id == client_id))

        return list(res.all())

    async def get_datasource_by_id(self, ds_id: str) -> DataSourceView:
        """Can raise NoResultsFound."""
        res = await self.session.execute(
            select(DataSource).where(
                DataSource.datasource_id == ds_id,
                DataSource.removed == False,
            )
        )
        return view(res.scalar_one())

    async def get_datasource_by_name(self, ds_name: str) -> list[DataSourceView]:
        res = await self.session.scalars(
            select(DataSource).where(DataSource.name == ds_name, DataSource.removed == False)
        )
        return [view(d) for d in res.all()]

    async def get_client_by_datasource_id(self, ds_id: str) -> Client:
        res = await self.session.execute(
            select(ComponentDB)
            .join(DataSource, ComponentDB.component_id == DataSource.component_id)
            .where(
                DataSource.datasource_id == ds_id,
                DataSource.removed == False,
            )
        )
        return viewClient(res.scalar_one())

    async def get_features_by_datasource(self, ds: DataSourceView) -> list[Feature]:
        res = await self.session.scalars(
            select(Feature).where(
                Feature.datasource_id == ds.datasource_id,
                Feature.removed == False,
            )
        )
        return list(res.all())
