import logging
from datetime import datetime
from uuid import uuid4

from sqlalchemy import select

from ferdelance.shared.artifacts import Metadata, MetaDataSource, MetaFeature

from ..schemas import Client as ClientView
from ..schemas import ClientDataSource as ClientDataSourceView
from ..tables import Client, ClientDataSource, ClientFeature
from .client import get_view
from .core import AsyncSession, DBSessionService

LOGGER = logging.getLogger(__name__)


def get_view(datasource: ClientDataSource) -> ClientDataSourceView:
    return ClientDataSourceView(**datasource.__dict__)


class DataSourceService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_or_update_metadata(self, client_id: str, metadata: Metadata) -> None:
        for ds in metadata.datasources:
            await self.create_or_update_datasource(client_id, ds)

    async def create_or_update_datasource(self, client_id: str, ds: MetaDataSource) -> ClientDataSource:
        dt_now = datetime.now()

        res = await self.session.execute(
            select(ClientDataSource).where(
                ClientDataSource.client_id == client_id,
                ClientDataSource.name == ds.name,
            )
        )

        # check if ds exists:
        ds_db: ClientDataSource | None = res.scalar_one_or_none()

        if ds_db is None:
            # create a new data source for this client
            LOGGER.info(f"client_id={client_id}: creating new data source={ds.name}")

            ds_db = ClientDataSource(
                datasource_id=str(uuid4()),
                name=ds.name,
                n_records=ds.n_records,
                n_features=ds.n_features,
                client_id=client_id,
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
                x = await self.session.execute(
                    select(ClientFeature).where(ClientFeature.datasource_id == ds_db.client_id)
                )

                features: list[ClientFeature] = x.scalars().all()

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
        self,
        ds: ClientDataSource,
        f: MetaFeature,
        remove: bool = False,
        commit: bool = True,
    ) -> ClientFeature:
        dt_now = datetime.now()

        res = await self.session.execute(
            select(ClientFeature).where(
                ClientFeature.datasource_id == ds.datasource_id,
                ClientFeature.name == f.name,
            )
        )

        f_db: ClientFeature | None = res.scalar_one_or_none()

        if f_db is None:
            LOGGER.info(f"client_id={ds.datasource_id}: creating new feature={f.name}")

            f_db = ClientFeature(
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

    async def get_datasource_list(self) -> list[ClientDataSourceView]:
        res = await self.session.scalars(select(ClientDataSource))
        return [get_view(r) for r in res.all()]

    async def get_datasource_by_client_id(self, client_id: str) -> list[ClientDataSourceView]:
        res = await self.session.scalars(select(ClientDataSource).where(ClientDataSource.client_id == client_id))
        return [get_view(r) for r in res.all()]

    async def get_datasource_ids_by_client_id(self, client_id: str) -> list[str]:
        res = await self.session.scalars(
            select(ClientDataSource.datasource_id).where(ClientDataSource.client_id == client_id)
        )

        return res.all()

    async def get_datasource_by_id(self, ds_id: str) -> ClientDataSourceView | None:
        res = await self.session.execute(
            select(ClientDataSource).where(
                ClientDataSource.datasource_id == ds_id,
                ClientDataSource.removed == False,
            )
        )
        return get_view(res.scalar_one())

    async def get_datasource_by_name(self, ds_name: str) -> list[ClientDataSource]:
        res = await self.session.scalars(
            select(ClientDataSource).where(ClientDataSource.name == ds_name, ClientDataSource.removed == False)
        )
        return res.all()

    async def get_client_by_datasource_id(self, ds_id: str) -> ClientView:
        res = await self.session.execute(
            select(Client)
            .join(ClientDataSource, Client.client_id == ClientDataSource.client_id)
            .where(
                ClientDataSource.datasource_id == ds_id,
                ClientDataSource.removed == False,
            )
        )
        return get_view(res.scalar_one())

    async def get_features_by_datasource(self, ds: ClientDataSource) -> list[ClientFeature]:
        res = await self.session.scalars(
            select(ClientFeature).where(
                ClientFeature.datasource_id == ds.datasource_id,
                ClientFeature.removed == False,
            )
        )
        return res.all()
