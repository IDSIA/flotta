from ferdelance.database.tables import (
    DataSource as DataSourceDB,
    Feature as FeatureDB,
    Project as ProjectDB,
)
from ferdelance.database.services.component import viewClient, ComponentDB, Client
from ferdelance.database.services.core import AsyncSession, DBSessionService
from ferdelance.schemas.metadata import Metadata, MetaDataSource, MetaFeature
from ferdelance.schemas.datasources import DataSource

from datetime import datetime
from uuid import uuid4

from sqlalchemy import select

import logging

LOGGER = logging.getLogger(__name__)


def view(datasource: DataSourceDB) -> DataSource:
    return DataSource(
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
            await self.create_or_update_datasource(client_id, ds, False)
        await self.session.commit()

        LOGGER.info(f"client_id={client_id}: added {len(metadata.datasources)} new datasources")

    async def create_or_update_datasource(self, client_id: str, ds: MetaDataSource, commit: bool = True) -> DataSource:
        dt_now = datetime.now()

        res = await self.session.execute(
            select(DataSourceDB).where(
                DataSourceDB.component_id == client_id,
                DataSourceDB.datasource_hash == ds.datasource_hash,
            )
        )

        # check if ds exists:
        ds_db: DataSourceDB | None = res.scalar_one_or_none()

        if ds_db is None:
            # create a new data source for this client
            LOGGER.info(f"client_id={client_id}: creating new data source={ds.name}")

            ds.datasource_id = str(uuid4())

            ds_db = DataSourceDB(
                datasource_id=ds.datasource_id,
                datasource_hash=ds.datasource_hash,
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
                x = await self.session.execute(select(FeatureDB).where(FeatureDB.datasource_id == ds_db.component_id))

                features: list[FeatureDB] = list(x.scalars().all())

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

        for f in ds.features:
            await self.create_or_update_feature(ds_db, f, ds.removed, commit=False)

        if commit:
            await self.session.commit()
            await self.session.refresh(ds_db)

        return view(ds_db)

    async def create_or_update_feature(
        self, ds: DataSourceDB, f: MetaFeature, remove: bool = False, commit: bool = True
    ) -> FeatureDB:
        dt_now = datetime.now()

        res = await self.session.execute(
            select(FeatureDB).where(
                FeatureDB.datasource_id == ds.datasource_id,
                FeatureDB.name == f.name,
            )
        )

        f_db: FeatureDB | None = res.scalar_one_or_none()

        if f_db is None:
            LOGGER.info(f"datasource_id={ds.datasource_id}: creating new feature={f.name}")

            f_db = FeatureDB(
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
                datasource=ds,
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

    async def get_datasource_list(self) -> list[DataSource]:
        res = await self.session.scalars(select(DataSourceDB))
        return [view(d) for d in res.all()]

    async def get_datasource_by_client_id(self, client_id: str) -> list[DataSource]:
        res = await self.session.scalars(select(DataSourceDB).where(DataSourceDB.component_id == client_id))
        return [view(d) for d in res.all()]

    async def get_datasource_ids_by_client_id(self, client_id: str) -> list[str]:
        res = await self.session.scalars(
            select(DataSourceDB.datasource_id).where(DataSourceDB.component_id == client_id)
        )

        return list(res.all())

    async def get_datasource_by_id(self, ds_id: str) -> DataSource:
        """Can raise NoResultsFound."""
        res = await self.session.scalars(
            select(DataSourceDB).where(
                DataSourceDB.datasource_id == ds_id,
                DataSourceDB.removed == False,
            )
        )
        return view(res.one())

    async def get_datasource_by_name(self, ds_name: str) -> list[DataSource]:
        res = await self.session.scalars(
            select(DataSourceDB).where(DataSourceDB.name == ds_name, DataSourceDB.removed == False)
        )
        return [view(d) for d in res.all()]

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

    async def get_features_by_datasource(self, ds: DataSource) -> list[FeatureDB]:
        res = await self.session.scalars(
            select(FeatureDB).where(
                FeatureDB.datasource_id == ds.datasource_id,
                FeatureDB.removed == False,
            )
        )
        return list(res.all())

    # async def get_tokens_by_datasource(self, ds: DataSource) -> list[str]:
    #     res = await self.session.scalars(
    #         select(Project.token)
    #         .join(, .project_id == Project.project_id)
    #         .where(.datasource_id == ds.datasource_id)
    #     )
    #     return list(res.all())
