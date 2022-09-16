from http.client import HTTPException
from zipapp import create_archive
from fastapi import APIRouter, Depends

from sqlalchemy.orm import Session

from ...database import get_db, crud
from ...database.tables import Client, ClientDataSource
from ..schemas.workbench import *

import logging

LOGGER = logging.getLogger(__name__)

workbench_router = APIRouter()


@workbench_router.get('/workbench/')
async def wb_home():
    return 'Workbench 🔧'


@workbench_router.get('/workbench/client/list', response_model=list[str])
async def wb_get_client_list(db: Session = Depends(get_db)):
    clients: list[Client] = crud.get_client_list(db)

    return [m.client_id for m in clients]


@workbench_router.get('/workbench/client/{client_id}', response_model=ClientDetails)
async def wb_get_client_detail(client_id: str, db: Session = Depends(get_db)):
    client: Client = crud.get_client_by_id(db, client_id)

    return ClientDetails(
        client_id=client.client_id,
        created_at=client.creation_time,
        version=client.version
    )


@workbench_router.get('/workbench/datasource/list', response_model=list[int])
async def wb_get_datasource_list(db: Session = Depends(get_db)):
    ds_db: list[ClientDataSource] = crud.get_datasource_list(db)

    LOGGER.info(f'found {len(ds_db)} datasource(s)')

    return [ds.datasource_id for ds in ds_db]


@workbench_router.get('/workbench/datasource/{ds_id}', response_model=DataSourceDetails)
async def wb_get_client_datasource(ds_id: int, db: Session = Depends(get_db)):
    ds_db, f_db = crud.get_datasource_by_id(db, ds_id)

    if ds_db is None:
        raise HTTPException(404)

    ds = DataSource(
        datasource_id=ds_db.datasource_id,
        name=ds_db.name,
        type=ds_db.type,
        created_at=ds_db.creation_time,
        n_records=ds_db.n_records,
        n_features=ds_db.n_features,
        client_id=ds_db.client_id,
    )

    fs = [Feature(
        feature_id=f.feature_id,
        name=f.name,
        dtype=f.dtype,
        created_at=f.creation_time,
        v_mean=f.v_mean,
        v_std=f.v_std,
        v_min=f.v_min,
        v_p25=f.v_p25,
        v_p50=f.v_p50,
        v_p75=f.v_p75,
        v_max=f.v_max,
        v_miss=f.v_miss,
    ) for f in f_db]

    return DataSourceDetails(
        datasource=ds,
        features=fs,
    )
