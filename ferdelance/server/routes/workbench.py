from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, Response

from sqlalchemy.exc import SQLAlchemyError, MultipleResultsFound, NoResultFound

from ...database import get_session, AsyncSession
from ...database.services import (
    ArtifactService,
    ClientService,
    DataSourceService,
    UserService,
)
from ...database.schemas import Client, User
from ...database.tables import (
    ClientDataSource,
    Model,
    UserToken,
)
from ..services import (
    JobManagementService,
    SecurityService,
    TokenService,
)
from ..security import check_user_token

from ferdelance.shared.artifacts import (
    DataSource,
    Feature,
    ArtifactStatus,
    Artifact,
)
from ferdelance.shared.schemas import (
    ClientDetails,
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchDataSourceList,
    WorkbenchJoinRequest,
    WorkbenchJoinData,
)
from ferdelance.shared.decode import decode_from_transfer

import logging
import os

LOGGER = logging.getLogger(__name__)


workbench_router = APIRouter()


@workbench_router.get('/workbench/')
async def wb_home():
    return 'Workbench 🔧'


@workbench_router.post('/workbench/connect', response_class=Response)
async def wb_connect(data: WorkbenchJoinRequest, session: AsyncSession = Depends(get_session)):
    LOGGER.info('new workbench connected')

    us: UserService = UserService(session)
    ss: SecurityService = SecurityService(session)
    ts: TokenService = TokenService(session)

    try:
        user_public_key = decode_from_transfer(data.public_key)

        try:
            user: User = await us.get_user_by_key(user_public_key)

            user_token: UserToken | None = await us.get_user_token_by_user_id(user.user_id)

            if user_token is None:
                raise HTTPException(403, 'Invalid user access')

        except NoResultFound:
            # creating new user
            user_token: UserToken | None = await ts.generate_user_token()
            user = await us.create_user(
                user_id=user_token.user_id,
                public_key=user_public_key,
            )
            user_token = await us.create_user_token(user_token)

            LOGGER.info(f'user_id={user.user_id}: created new user')

        LOGGER.info(f'user_id={user.user_id}: new workbench connected')

        await ss.setup(user.public_key)

        wjd = WorkbenchJoinData(
            id=user.user_id,
            token=user_token.token,
            public_key=ss.get_server_public_key()
        )

        return ss.create_response(wjd.dict())

    except SQLAlchemyError as e:
        LOGGER.exception(e)
        LOGGER.exception('Database error')
        raise HTTPException(500, 'Internal error')

    except ValueError as e:
        LOGGER.exception(e)
        raise HTTPException(403, 'Invalid client data')


@workbench_router.get('/workbench/client/list', response_class=Response)
async def wb_get_client_list(session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}: requested a list of clients')

    cs: ClientService = ClientService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    clients: list[Client] = await cs.get_client_list()

    wcl = WorkbenchClientList(
        client_ids=[c.client_id for c in clients if c.active is True]
    )

    return ss.create_response(wcl.dict())


@workbench_router.get('/workbench/client/{req_client_id}', response_class=Response)
async def wb_get_user_detail(req_client_id: str, session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}: requested details on client_id={req_client_id}')

    cs: ClientService = ClientService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    client: Client = await cs.get_client_by_id(req_client_id)

    if client.active is False:
        LOGGER.warning(f'client_id={req_client_id} not found in database or is not active')
        raise HTTPException(404)

    cd = ClientDetails(
        client_id=client.client_id,
        version=client.version
    )

    return ss.create_response(cd.dict())


@workbench_router.get('/workbench/datasource/list', response_class=Response)
async def wb_get_datasource_list(session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}: requested a list of available data source')

    dss: DataSourceService = DataSourceService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    ds_session: list[ClientDataSource] = await dss.get_datasource_list()

    LOGGER.info(f'found {len(ds_session)} datasource(s)')

    wdsl = WorkbenchDataSourceIdList(
        datasource_ids=[ds.datasource_id for ds in ds_session if ds.removed is False]
    )

    return ss.create_response(wdsl.dict())


@workbench_router.get('/workbench/datasource/{ds_id}', response_class=Response)
async def wb_get_client_datasource(ds_id: str, session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}: requested details on datasource_id={ds_id}')

    dss: DataSourceService = DataSourceService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    ds_session: ClientDataSource | None = await dss.get_datasource_by_id(ds_id)

    if ds_session is None or ds_session.removed is True:
        LOGGER.warning(f'datasource_id={ds_id} not found in database or has been removed')
        raise HTTPException(404)

    f_session = await dss.get_features_by_datasource(ds_session)

    fs = [Feature(**f.__dict__) for f in f_session if not f.removed]

    ds = DataSource(
        **ds_session.__dict__,
        features=fs,
    )

    return ss.create_response(ds.dict())


@workbench_router.get('/workbench/datasource/name/{ds_name}', response_class=Response)
async def wb_get_client_datasource_by_name(ds_name: str, session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}: requested details on datasource_name={ds_name}')

    dss: DataSourceService = DataSourceService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    ds_dbs: list[ClientDataSource] = await dss.get_datasource_by_name(ds_name)

    if not ds_dbs:
        LOGGER.warning(f'datasource_id={ds_name} not found in database or has been removed')
        raise HTTPException(404)

    ret_ds: list[DataSource] = []

    for ds_db in ds_dbs:

        f_db = await dss.get_features_by_datasource(ds_db)

        fs = [Feature(**f.__dict__) for f in f_db if not f.removed]

        ret_ds.append(DataSource(
            **ds_db.__dict__,
            features=fs,
        ))

    wdsl = WorkbenchDataSourceList(
        datasources=ret_ds
    )

    return ss.create_response(wdsl.dict())


@workbench_router.post('/workbench/artifact/submit', response_class=Response)
async def wb_post_artifact_submit(request: Request, session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}:  submitted a new artifact')

    try:
        jms: JobManagementService = JobManagementService(session)
        ss: SecurityService = SecurityService(session)

        await ss.setup(user.public_key)

        data = await ss.read_request(request)
        artifact = Artifact(**data)

        status = await jms.submit_artifact(artifact)

        LOGGER.info(f'submitted artifact got artifact_id={status.artifact_id}')

        return ss.create_response(status.dict())

    except ValueError as e:
        LOGGER.error('Artifact already exists')
        LOGGER.exception(e)
        raise HTTPException(403)


@workbench_router.get('/workbench/artifact/status/{artifact_id}', response_class=Response)
async def wb_get_artifact_status(artifact_id: str, session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}:  requested status of artifact_id={artifact_id}')

    ars: ArtifactService = ArtifactService(session)
    ss: SecurityService = SecurityService(session)

    await ss.setup(user.public_key)

    artifact_session = await ars.get_artifact(artifact_id)

    # TODO: get status from celery

    if artifact_session is None:
        LOGGER.warning(f'artifact_id={artifact_id} not found in database')
        raise HTTPException(404)

    status = ArtifactStatus(
        artifact_id=artifact_id,
        status=artifact_session.status,
    )

    return ss.create_response(status.dict())


@workbench_router.get('/workbench/artifact/{artifact_id}', response_class=Response)
async def wb_get_artifact(artifact_id: str, session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}: requested details on artifact_id={artifact_id}')

    try:
        jms: JobManagementService = JobManagementService(session)
        ss: SecurityService = SecurityService(session)

        await ss.setup(user.public_key)

        artifact = await jms.get_artifact(artifact_id)

        return ss.create_response(artifact.dict())

    except ValueError as e:
        LOGGER.error(f'{e}')
        raise HTTPException(404)


@workbench_router.get('/workbench/model/{artifact_id}', response_class=FileResponse)
async def wb_get_model(artifact_id: str, session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}: requested aggregate model for artifact_id={artifact_id}')

    try:
        ars: ArtifactService = ArtifactService(session)
        ss: SecurityService = SecurityService(session)

        await ss.setup(user.public_key)

        model_session: Model = await ars.get_aggregated_model(artifact_id)

        model_path = model_session.path

        if not os.path.exists(model_path):
            raise ValueError(f'model_id={model_session.model_id} not found at path={model_path}')

        return ss.encrypt_file(model_path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except NoResultFound as _:
        LOGGER.warning(f'no aggregated model found for artifact_id={artifact_id}')
        raise HTTPException(404)

    except MultipleResultsFound as _:
        LOGGER.error(f'multiple aggregated models found for artifact_id={artifact_id}')  # TODO: do we want to allow this?
        raise HTTPException(500)


@workbench_router.get('/workbench/model/partial/{artifact_id}/{builder_user_id}', response_class=FileResponse)
async def wb_get_partial_model(artifact_id: str, builder_user_id: str, session: AsyncSession = Depends(get_session), user: User = Depends(check_user_token)):
    LOGGER.info(f'user_id={user.user_id}: requested partial model for artifact_id={artifact_id} from user_id={builder_user_id}')

    try:
        ars: ArtifactService = ArtifactService(session)
        ss: SecurityService = SecurityService(session)

        await ss.setup(user.public_key)

        model_session: Model = await ars.get_partial_model(artifact_id, builder_user_id)

        model_path = model_session.path

        if not os.path.exists(model_path):
            raise ValueError(f'partial model_id={model_session.model_id} not found at path={model_path}')

        return ss.encrypt_file(model_path)

    except ValueError as e:
        LOGGER.warning(str(e))
        raise HTTPException(404)

    except sqlex.NoResultFound as _:
        LOGGER.warning(f'no partial model found for artifact_id={artifact_id} and user_id={builder_user_id}')
        raise HTTPException(404)

    except sqlex.MultipleResultsFound as _:
        LOGGER.error(f'multiple partial models found for artifact_id={artifact_id} and user_id={builder_user_id}')  # TODO: do we want to allow this?
        raise HTTPException(500)
