from http.client import HTTPException
from fastapi import APIRouter, Depends

from sqlalchemy.orm import Session

from ...database import get_db, crud
from ...database.tables import Client
from ..schemas.workbench import *

import logging

LOGGER = logging.getLogger(__name__)

workbench_router = APIRouter()


@workbench_router.get('/workbench/client/list')
async def wb_get_client_list(db: Session = Depends(get_db)):
    clients: list[Client] = crud.get_client_list(db)

    return [m.client_id for m in clients]


@workbench_router.get('/workbench/client/{client_id}')
async def wb_get_client_detail(client_id: str, db: Session = Depends(get_db)):
    return crud.get_client_by_id(db, client_id)


@workbench_router.get('/workbench/datasource/list')
async def wb_get_datasource_list(db: Session = Depends(get_db)):
    return crud.get_datasource_list(db)


@workbench_router.get('/workbench/datasource/{ds_id}')
async def wb_get_client_datasource(ds_id: int, db: Session = Depends(get_db)):
    ds, features = crud.get_datasource_by_id(db, ds_id)

    if ds is None:
        raise HTTPException(404)

    return {
        'datasource': ds,
        'features': features,
    }
