from sqlalchemy.orm import Session

from .tables import Client, ClientEvent, ClientToken, ClientApp, Model, ClientDataSource, ClientFeature

from datetime import datetime

import logging

LOGGER = logging.getLogger(__name__)


def create_client(db: Session, client: Client) -> Client:
    LOGGER.info(f'creating new client with version={client.version} mac_address={client.machine_mac_address} node={client.machine_node}')

    existing_client_id = (
        db.query(Client.client_id)
        .filter(
                (Client.machine_mac_address == client.machine_mac_address) |
                (Client.machine_node == client.machine_node)
        )
        .first()
    )
    if existing_client_id is not None:
        LOGGER.warning(f'client already exists with id {existing_client_id}')
        raise ValueError('Client already exists')

    db.add(client)
    db.commit()
    db.refresh(client)

    return client


def update_client(db: Session, client_id: str, version: str = None) -> None:
    u = dict()

    if version is not None:
        LOGGER.info(f'client_id={client_id}: update version to {version}')
        u['version'] = version

    if not u:
        return

    db.query(Client).filter(Client.client_id == client_id).update(u)
    db.commit()


def client_leave(db: Session, client_id: str) -> Client:
    db.query(Client).filter(Client.client_id == client_id).update({
        'active': False,
        'left': True,
    })
    invalidate_all_tokens(db, client_id)  # this will already commit the changes!


def get_client_by_id(db: Session, client_id: str) -> Client:
    return db.query(Client).filter(Client.client_id == client_id).first()


def get_client_list(db: Session) -> list[Client]:
    return db.query(Client).all()


def get_client_by_token(db: Session, token: str) -> Client:
    return db.query(Client)\
        .join(ClientToken, Client.client_id == ClientToken.token_id)\
        .filter(ClientToken.token == token)\
        .first()


def create_client_token(db: Session, token: ClientToken) -> ClientToken:
    LOGGER.info(f'client_id={token.client_id}: creating new token')

    existing_client_id = db.query(ClientToken.client_id).filter(ClientToken.token == token.token).first()

    if existing_client_id is not None:
        LOGGER.warning(f'valid token already exists for client_id {existing_client_id}')
        # TODO: check if we have more strong condition for this
        return

    db.add(token)
    db.commit()
    db.refresh(token)

    return token


def invalidate_all_tokens(db: Session, client_id: str) -> None:
    db.query(ClientToken).filter(ClientToken.client_id == client_id).update({
        'valid': False,
    })
    db.commit()


def get_client_id_by_token(db: Session, token: str) -> str:
    return db.query(ClientToken.client_id).filter(ClientToken.token == token).first()


def get_client_token_by_token(db: Session, token: str) -> ClientToken:
    return db.query(ClientToken).filter(ClientToken.token == token).first()


def get_client_token_by_client_id(db: Session, client_id: str) -> ClientToken:
    return db.query(ClientToken).filter(ClientToken.client_id == client_id).first()


def create_client_event(db: Session, client_id: str, event: str) -> ClientEvent:
    LOGGER.info(f'client_id={client_id}: creating new event="{event}"')

    db_client_event = ClientEvent(
        client_id=client_id,
        event=event
    )

    db.add(db_client_event)
    db.commit()
    db.refresh(db_client_event)

    return db_client_event


def get_all_client_events(db: Session, client: Client) -> list[ClientEvent]:
    LOGGER.info(f'client_id={client.client_id}: requested all events')

    return db.query(ClientEvent).filter(ClientEvent.client_id == client.client_id).all()


def get_newest_app_version(db: Session) -> ClientApp:
    db_client_app: ClientApp = db.query(ClientApp)\
        .filter(ClientApp.active)\
        .order_by(ClientApp.creation_time.desc())\
        .first()

    if db_client_app is None:
        return None

    return db_client_app


def get_newest_app(db: Session) -> ClientApp:
    return db.query(ClientApp)\
        .filter(ClientApp.active)\
        .order_by(ClientApp.creation_time.desc())\
        .first()


def get_model_by_id(db: Session, model_id: int) -> Model | None:
    return db.query(Model).filter(Model.model_id == model_id).first()


def create_or_update_datasource(db: Session, client_id: str, ds: dict) -> ClientDataSource:
    ds_name = ds['name']
    ds_removed = ds['removed'] if 'removed' in ds else False

    dt_now = datetime.now()

    query = db.query(ClientDataSource).filter(
        ClientDataSource.client_id == client_id,
        ClientDataSource.name == ds_name,
    )

    # check if ds exists:
    ds_db: ClientDataSource = query.first()

    if ds_db is None:
        # create a new data source for this client
        LOGGER.info(f'creating new data source={ds_name} for client_id={client_id}')

        ds_db = ClientDataSource(
            name=ds['name'],
            type=ds['type'],
            n_records=ds['n_records'],
            n_features=ds['n_features'],
            client_id=client_id,
        )

        db.add(ds_db)

    else:
        if ds_removed:
            # remove data source and info
            LOGGER.info(f'removing data source={ds_name} for client_id={client_id}')

            query.update({
                'removed': True,
                'type': None,
                'n_records': None,
                'n_features': None,
                'update_time': dt_now,
            })

            # remove features assigned with this data source
            db.query(ClientFeature)\
                .filter(ClientFeature.datasource_id == ds_db.client_id)\
                .update({
                    'removed': True,
                    'dtype': None,
                    'v_mean': None,
                    'v_std ': None,
                    'v_min ': None,
                    'v_p25 ': None,
                    'v_p50 ': None,
                    'v_p75 ': None,
                    'v_max ': None,
                    'v_miss': None,
                    'update_time': dt_now,
                })

        else:
            # update data source info
            LOGGER.info(f'updating data source={ds_name} for client_id={client_id}')
            query.update({
                'type': ds['type'],
                'n_records': ds['n_records'],
                'n_features': ds['n_features'],
                'update_time': dt_now,
            })

    db.commit()
    db.refresh(ds_db)

    if not ds_removed:
        for f in ds['features']:
            create_or_update_feature(db, ds_db.datasource_id, f, False)

        db.commit()

    return ds_db


def create_or_update_feature(db: Session, ds_id: str, f: dict, commit=True) -> ClientFeature:
    f_name = f['name']
    f_removed = f['removed'] if 'removed' in f else False

    dt_now = datetime.now()

    query = db.query(ClientFeature).filter(
        ClientFeature.datasource_id == ds_id,
        ClientFeature.name == f_name
    )

    f_db = query.first()

    if f_db is None:
        LOGGER.info(f'creating new feature={f_name} for client_id={ds_id}')

        f_db = ClientFeature(
            name=f['name'],
            dtype=f['dtype'],
            v_mean=f['v_mean'],
            v_std=f['v_std'],
            v_min=f['v_min'],
            v_p25=f['v_p25'],
            v_p50=f['v_p50'],
            v_p75=f['v_p75'],
            v_miss=f['v_miss'],
            v_max=f['v_max'],
            removed=f_removed,
            datasource_id=ds_id,
        )

        db.add(f_db)
    else:
        if f_removed:
            # remove feature and info
            LOGGER.info(f'removing feature={f_name} for datasource={ds_id}')

            query.update({
                'removed': True,
                'dtype': None,
                'v_mean': None,
                'v_std ': None,
                'v_min ': None,
                'v_p25 ': None,
                'v_p50 ': None,
                'v_p75 ': None,
                'v_max ': None,
                'v_miss': None,
                'update_time': dt_now,
            })

        else:
            # update data source info
            LOGGER.info(f'updating data source={f_name} for client_id={ds_id}')
            query.update({
                'type': f['type'],
                'dtype': f['dtype'],
                'v_mean': f['v_mean'],
                'v_std ': f['v_std'],
                'v_min ': f['v_min'],
                'v_p25 ': f['v_p25'],
                'v_p50 ': f['v_p50'],
                'v_p75 ': f['v_p75'],
                'v_max ': f['v_max'],
                'v_miss': f['v_miss'],
                'update_time': dt_now,
            })

    if commit:
        db.commit()
        db.refresh(f_db)

        return f_db

    return None
