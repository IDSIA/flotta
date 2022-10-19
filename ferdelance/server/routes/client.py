from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import Response

from sqlalchemy.exc import SQLAlchemyError

from ...database import get_db
from ...database.tables import Client, ClientApp, ClientDataSource, ClientToken
from ..services.actions import ActionService
from ..services.security import SecurityService
from ...database.services import (
    Session,
    ClientAppService,
    ClientService,
    DataSourceService,
    ModelService,
)
from ...database.tables import Client, ClientApp, ClientToken, Model
from ..services import ActionService, SecurityService, JobManagementService
from ..security import check_token
from ..exceptions import ArtifactDoesNotExists, TaskDoesNotExists

from ferdelance_shared.schemas import (
    ClientJoinRequest,
    ClientJoinData,
    DownloadApp,
    MetaFeature,
    Metadata,
    MetaDataSource,
    UpdateExecute,
)
from ferdelance_shared.schemas.models import Metrics

from typing import Any

import logging
import json

LOGGER = logging.getLogger(__name__)


client_router = APIRouter()


@client_router.post('/client/join', response_class=Response)
async def client_join(request: Request, client: ClientJoinRequest, db: Session = Depends(get_db)):
    """API for new client joining."""
    LOGGER.info('new client join request')

    cs: ClientService = ClientService(db)
    ss: SecurityService = SecurityService(db, None)

    try:
        if request.client is None:
            LOGGER.warn('client not set for request?')
            raise HTTPException(400)

        ip_address = request.client.host

        client_token: ClientToken = ss.generate_token(client.system, client.mac_address, client.node)

        token = client_token.token
        client_id = client_token.client_id

        client = Client(
            client_id=client_id,
            version=client.version,
            public_key=client.public_key,
            machine_system=client.system,
            machine_mac_address=client.mac_address,
            machine_node=client.node,
            ip_address=ip_address,
            type='CLIENT',
        )

        ss.client = cs.create_client(client)
        client_token = cs.create_client_token(client_token)

        cs.create_client_event(client_id, 'creation')

        LOGGER.info(f'client_id={client_id}: joined')

        cjd = ClientJoinData(
            id=client_id,
            token=token,
            public_key=ss.get_server_public_key_str(),
        )

        return ss.server_encrypt_response(cjd.dict())

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception('Database error')
        raise HTTPException(500, 'Internal error')

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, 'Invalid client data')


@client_router.post('/client/leave')
async def client_leave(db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """API for existing client to be removed"""
    cs: ClientService = ClientService(db)

    LOGGER.info(f'client_id={client_id}: request to leave')

    cs.client_leave(client_id)
    cs.create_client_event(client_id, 'left')

    return {}


@client_router.get('/client/update', response_class=Response)
async def client_update(request: Request, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new artifact package
    - new client app package
    - nothing (keep alive)
    """
    acs: ActionService = ActionService(db)
    cs: ClientService = ClientService(db)
    ss: SecurityService = SecurityService(db, client_id)

    cs.create_client_event(client_id, 'update')

    # consume current results (if present) and compute next action
    data = await request.body()
    payload: dict[str, Any] = ss.server_decrypt_json_content(data)

    next_action = acs.next(ss.client, payload)

    LOGGER.info(f'client_id={client_id}: update action={next_action.action}')

    cs.create_client_event(client_id, f'action:{next_action.action}')

    return ss.server_encrypt_response(next_action.dict())


@client_router.get('/client/download/application', response_class=Response)
async def client_update_files(request: Request, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """
    API request by the client to get updated files. With this endpoint a client can:
    - update application software
    - obtain model files
    """
    LOGGER.info(f'client_id={client_id}: update files request')

    cas: ClientAppService = ClientAppService(db)
    cs: ClientService = ClientService(db)
    ss: SecurityService = SecurityService(db, client_id)

    cs.create_client_event(client_id, 'update files')

    body = await request.body()
    payload = DownloadApp(**ss.server_decrypt_json_content(body))

    new_app: ClientApp = cas.get_newest_app()

    if new_app is None:
        raise HTTPException(400, 'no newest version found')

    if new_app.version != payload.version:
        LOGGER.warning(f'client_id={client_id} requested app version={payload.version} while latest version={new_app.version}')
        raise HTTPException(400, 'Old versions are not permitted')

    cs.update_client(client_id, version=payload.version)

    LOGGER.info(f'client_id={client_id}: requested new client version={payload.version}')

    return ss.server_stream_encrypt_file(new_app.path)


@client_router.post('/client/update/metadata')
async def client_update_metadata(request: Request, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    """Endpoint used by a client to send information regarding its metadata. These metadata includes:
    - data source available
    - summary (source, data type, min value, max value, standard deviation, ...) of features available for each data source
    """
    LOGGER.info(f'client_id={client_id}: update metadata request')

    cs: ClientService = ClientService(db)
    dss: DataSourceService = DataSourceService(db)
    ss: SecurityService = SecurityService(db, client_id)

    cs.create_client_event(client_id, 'update metadata')

    body = await request.body()
    metadata = Metadata(**ss.server_decrypt_json_content(body))

    dss.create_or_update_metadata(client_id, metadata)

    client_data_source_list: list[ClientDataSource] = dss.get_datasource_by_client_id(client_id)

    ds_list = [
        MetaDataSource(
            **cds.__dict__,
            features=[
                MetaFeature(**f.__dict__) for f in dss.get_features_by_datasource(cds)
            ])
        for cds in client_data_source_list
    ]

    return Response(ss.server_encrypt_content(json.dumps([ds.dict() for ds in ds_list])))


@client_router.get('/client/task', response_class=Response)
async def client_get_task(request: Request, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    LOGGER.info(f'client_id={client_id}: new task request')

    cs: ClientService = ClientService(db)
    jm: JobManagementService = JobManagementService(db)
    ss: SecurityService = SecurityService(db, client_id)

    cs.create_client_event(client_id, 'schedule task')

    body = await request.body()
    payload = UpdateExecute(**ss.server_decrypt_json_content(body))
    artifact_id = payload.artifact_id

    try:
        content = await jm.client_local_model_start(artifact_id, client_id)

    except ArtifactDoesNotExists as _:
        raise HTTPException(404, 'Artifact does not exists')

    except TaskDoesNotExists as _:
        raise HTTPException(404, 'Task does not exists')

    return ss.server_encrypt_response(content.dict())

# TODO: add endpoint for failed job executions


@client_router.post('/client/task/{artifact_id}')
async def client_post_task(request: Request, artifact_id: str, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    LOGGER.info(f'client_id={client_id}: complete work on artifact_id={artifact_id}')

    ss: SecurityService = SecurityService(db, client_id)
    jm: JobManagementService = JobManagementService(db)
    ms: ModelService = ModelService(db)

    model_db: Model = ms.create_local_model(artifact_id, client_id)

    await ss.server_stream_decrypt_file(request, model_db.path)

    jm.client_local_model_completed(artifact_id, client_id)

    return {}


@client_router.post('/client/metrics')
async def client_post_metrics(request: Request, db: Session = Depends(get_db), client_id: str = Depends(check_token)):
    ss: SecurityService = SecurityService(db, client_id)
    jm: JobManagementService = JobManagementService(db)

    body = await request.body()
    metrics = Metrics(**ss.server_decrypt_json_content(body))

    LOGGER.info(f'client_id={client_id}: submitted new metrics for artifact_id={metrics.artifact_id} source={metrics.source}')

    jm.save_metrics(metrics)

    return {}
