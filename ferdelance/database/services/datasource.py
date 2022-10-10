from .core import DBSessionService, Session

from ..tables import ClientDataSource, ClientFeature, Client

from ferdelance_shared.schemas import Metadata, MetaDataSource, MetaFeature

from datetime import datetime
from uuid import uuid4

import logging

LOGGER = logging.getLogger(__name__)


class DataSourceService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def create_or_update_metadata(self, client_id: str, metadata: Metadata) -> None:
        for ds in metadata.datasources:
            self.create_or_update_datasource(client_id, ds)

    def create_or_update_datasource(self, client_id: str, ds: MetaDataSource) -> ClientDataSource:
        dt_now = datetime.now()

        query = self.db.query(ClientDataSource).filter(
            ClientDataSource.client_id == client_id,
            ClientDataSource.name == ds.name,
        )

        # check if ds exists:
        ds_db: ClientDataSource = query.one_or_none()

        if ds_db is None:
            # create a new data source for this client
            LOGGER.info(f'client_id={client_id}: creating new data source={ds.name}')

            ds_db = ClientDataSource(
                datasource_id=str(uuid4()),
                name=ds.name,
                n_records=ds.n_records,
                n_features=ds.n_features,
                client_id=client_id,
            )

            self.db.add(ds_db)

        else:
            if ds.removed:
                # remove data source and info
                LOGGER.info(f'client_id={client_id}: removing data source={ds.name}')

                query.update({
                    'removed': True,
                    'type': None,
                    'n_records': None,
                    'n_features': None,
                    'update_time': dt_now,
                })

                # remove features assigned with this data source
                self.db.query(ClientFeature)\
                    .filter(ClientFeature.datasource_id == ds_db.client_id)\
                    .update({
                        'removed': True,
                        'dtype': None,
                        'v_mean': None,
                        'v_std': None,
                        'v_min': None,
                        'v_p25': None,
                        'v_p50': None,
                        'v_p75': None,
                        'v_max': None,
                        'v_miss': None,
                        'update_time': dt_now,
                    })

            else:
                # update data source info
                LOGGER.info(f'client_id={client_id}: updating data source={ds.name}')
                query.update({
                    'n_records': ds.n_records,
                    'n_features': ds.n_features,
                    'update_time': dt_now,
                })

        self.db.commit()
        self.db.refresh(ds_db)

        for f in ds.features:
            self.create_or_update_feature(ds_db, f, ds.removed, commit=False)

        self.db.commit()

        return ds_db

    def create_or_update_feature(self, ds: ClientDataSource, f: MetaFeature, remove: bool = False, commit: bool = True) -> ClientFeature:
        dt_now = datetime.now()

        query = self.db.query(ClientFeature).filter(
            ClientFeature.datasource_id == ds.datasource_id,
            ClientFeature.name == f.name
        )

        f_db: ClientFeature | None = query.one_or_none()

        if f_db is None:
            LOGGER.info(f'client_id={ds.datasource_id}: creating new feature={f.name}')

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
                datasource_name=ds.name
            )

            self.db.add(f_db)
        else:
            if remove or f.removed:
                # remove feature and info
                LOGGER.info(f'removing feature={f.name} for datasource={ds.datasource_id}')

                query.update({
                    'removed': True,
                    'dtype': None,
                    'v_mean': None,
                    'v_std': None,
                    'v_min': None,
                    'v_p25': None,
                    'v_p50': None,
                    'v_p75': None,
                    'v_max': None,
                    'v_miss': None,
                    'update_time': dt_now,
                })

            else:
                # update data source info
                LOGGER.info(f'client_id={ds.datasource_id}: updating data source={f.name}')
                query.update({
                    'dtype': f.dtype,
                    'v_mean': f.v_mean,
                    'v_std': f.v_std,
                    'v_min': f.v_min,
                    'v_p25': f.v_p25,
                    'v_p50': f.v_p50,
                    'v_p75': f.v_p75,
                    'v_max': f.v_max,
                    'v_miss': f.v_miss,
                    'update_time': dt_now,
                })

        if commit:
            self.db.commit()
            self.db.refresh(f_db)

        return f_db

    def get_datasource_list(self) -> list[ClientDataSource]:
        return self.db.query(ClientDataSource).all()

    def get_datasource_by_client_id(self, client_id: str) -> list[ClientDataSource]:
        return self.db.query(ClientDataSource).filter(ClientDataSource.client_id == client_id).all()

    def get_datasource_ids_by_client_id(self, client_id: str) -> list[str]:
        return [r[0] for r in self.db.query(ClientDataSource.datasource_id).filter(ClientDataSource.client_id == client_id).all()]

    def get_datasource_by_id(self, ds_id: str) -> ClientDataSource:
        return self.db.query(ClientDataSource).filter(ClientDataSource.datasource_id == ds_id, ClientDataSource.removed == False).one()

    def get_client_by_datasource_id(self, ds_id: str) -> Client:
        return self.db.query(Client)\
            .join(ClientDataSource, Client.client_id == ClientDataSource.client_id)\
            .filter(ClientDataSource.datasource_id == ds_id, ClientDataSource.removed == False)\
            .one()

    def get_features_by_datasource(self, ds: ClientDataSource) -> list[ClientFeature]:
        return self.db.query(ClientFeature).filter(ClientFeature.datasource_id == ds.datasource_id, ClientFeature.removed == False).all()