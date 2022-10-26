from ferdelance.database.tables import (
    Client,
    ClientApp,
    ClientDataSource,
    ClientEvent,
    ClientFeature,
    ClientToken,
    Job,
)
from ferdelance.server.api import api
from ferdelance.config import STORAGE_ARTIFACTS

from ferdelance_shared.actions import Action
from ferdelance_shared.schemas import (
    Artifact,
    ArtifactStatus,
    ClientUpdate,
    ClientJoinRequest,
    Dataset,
    DownloadApp,
    Metadata,
    Query,
    QueryFeature,
    QueryFilter,
    UpdateClientApp,
    UpdateExecute,
    UpdateToken,
    WorkbenchJoinData,
)
from ferdelance_shared.models import Model
from ferdelance_shared.operations import Operations

from .utils import (
    setup_test_database,
    setup_rsa_keys,
    teardown_test_database,
    create_client,
    headers,
    bytes_from_public_key,
    get_payload,
    create_payload,
    decrypt_stream_response,
    get_metadata,
    send_metadata,
)

from fastapi.testclient import TestClient

from base64 import b64encode
from requests import Response
from sqlalchemy import select, update, delete, func
from sqlalchemy.orm import Session
from typing import Any

import json
import logging
import os
import random
import shutil

LOGGER = logging.getLogger(__name__)


def get_client_by_id(session: Session, client_id: str) -> Client | None:
    return session.execute(
        select(Client)
        .where(Client.client_id == client_id)
    ).scalar_one_or_none()


def get_token_by_id(session: Session, client_id: str) -> ClientToken | None:
    return session.execute(
        select(ClientToken)
        .where(
            ClientToken.client_id == client_id,
            ClientToken.valid == True
        )
    ).scalar_one_or_none()


def get_client_events(session: Session, client_id: str) -> list[str]:
    return session.scalars(
        select(ClientEvent.event)
        .where(ClientEvent.client_id == client_id)
    ).all()


class TestClientClass:

    def setup_class(self):
        """Class setup. This will be executed once each test. The setup will:
        - Create the client.
        - Create a new database on the remote server specified by `DB_HOST`, `DB_USER`, and `DB_PASS` (all env variables.).
            The name of the database is randomly generated using UUID4, if not supplied via `DB_SCHEMA` env variable.
            The database will be used as the server's database.
        - Populate this database with the required tables.
        - Generate and save to the database the servers' keys using the hardcoded `SERVER_MAIN_PASSWORD`.
        - Generate the local public/private keys to simulate a client application.
        """
        LOGGER.info('setting up')

        self.engine = setup_test_database()

        self.private_key = setup_rsa_keys()
        self.public_key = self.private_key.public_key()
        self.public_key_bytes = bytes_from_public_key(self.public_key)

        random.seed(42)

        self.server_key = None
        self.token = None

        LOGGER.info('setup completed')

    def teardown_class(self):
        """Class teardown. This method will ensure that the database is closed and deleted from the remote dbms.
        Note that all database connections still open will be forced to close by this method.
        """
        LOGGER.info('tearing down')

        teardown_test_database()

        LOGGER.info('teardown completed')

    def get_client(self, client: TestClient) -> str:
        client_id, self.token, self.server_key = create_client(client, self.private_key)
        return client_id

    def get_client_update(self, client: TestClient, payload: str | None = None, token: str | None = None) -> tuple[int, str, Any]:
        cur_token = token if token else self.token

        if payload is None:
            payload = json.dumps(ClientUpdate(action=Action.DO_NOTHING.name).dict())

        assert self.server_key is not None

        response = client.get(
            '/client/update',
            data=create_payload(self.server_key, payload),
            headers=headers(cur_token)
        )

        if response.status_code != 200:
            return response.status_code, '', None

        response_payload = get_payload(self.private_key, response.content)

        assert 'action' in response_payload

        return response.status_code, response_payload['action'], response_payload

    def test_read_home(self):
        """Generic test to check if the home works."""
        with TestClient(api) as client:
            create_client(client, self.private_key)

            response = client.get('/')

            assert response.status_code == 200
            assert response.content.decode('utf8') == '"Hi! 😀"'

    def test_client_connect_successful(self):
        """Simulates the arrival of a new client. The client will connect with a set of hardcoded values:
        - operative system
        - mac address
        - node identification number
        - its public key
        - the version of the software in use

        Then the server will answer with:
        - an encrypted token
        - an encrypted client id
        - a public key in str format
        """

        with TestClient(api) as client:
            client_id = self.get_client(client)

            with Session(self.engine) as session:
                db_client: Client | None = get_client_by_id(session, client_id)

                assert db_client is not None
                assert db_client.active

                db_token: ClientToken | None = get_token_by_id(session, client_id)

                assert db_token is not None
                assert db_token.token == self.token
                assert db_token.valid

                db_events: list[str] = get_client_events(session, client_id)

                assert len(db_events) == 1
                assert db_events[0] == 'creation'

    def test_client_already_exists(self):
        """This test will send twice the access information and expect the second time to receive a 403 error."""

        with TestClient(api) as client:
            client_id = self.get_client(client)
            public_key = bytes_from_public_key(self.public_key)

            with Session(self.engine) as session:
                client_db = get_client_by_id(session, client_id)

                assert client_db is not None

                data = ClientJoinRequest(
                    system=client_db.machine_system,
                    mac_address=client_db.machine_mac_address,
                    node=client_db.machine_node,
                    public_key=b64encode(public_key).decode('utf8'),
                    version='test',
                )

            response = client.post(
                '/client/join',
                json=data.dict()
            )

            assert response.status_code == 403
            assert response.json()['detail'] == 'Invalid client data'

    def test_client_update(self):
        """This will test the endpoint for updates."""

        with TestClient(api) as client:
            client_id = self.get_client(client)

            status_code, action, _ = self.get_client_update(client)

            assert status_code == 200
            assert Action[action] == Action.DO_NOTHING

            with Session(self.engine) as session:
                db_events: list[str] = get_client_events(session, client_id)

                assert len(db_events) == 3
                assert 'creation' in db_events
                assert 'update' in db_events
                assert f'action:{Action.DO_NOTHING.name}' in db_events

    def test_client_leave(self):
        """This will test the endpoint for leave a client."""
        with TestClient(api) as client:
            client_id = self.get_client(client)

            response_leave = client.post(
                '/client/leave',
                headers=headers(self.token)
            )

            LOGGER.info(f'response_leave={response_leave}')

            assert response_leave.status_code == 200

            # cannot get other updates
            status_code, _, _ = self.get_client_update(client)

            assert status_code == 403

            with Session(self.engine) as session:
                db_client: Client | None = get_client_by_id(session, client_id)

                assert db_client is not None
                assert db_client.active is False
                assert db_client.left

                db_events: list[str] = get_client_events(session, client_id)

                assert 'creation' in db_events
                assert 'left' in db_events
                assert 'update' not in db_events

    def test_client_update_token(self):
        """This will test the failure and update of a token."""
        with TestClient(api) as client:
            client_id = self.get_client(client)

            with Session(self.engine) as session:
                # expire token
                session.execute(
                    update(ClientToken)
                    .where(ClientToken.client_id == client_id)
                    .values(expiration_time=0)
                )
                session.commit()

                LOGGER.info('expiration_time for token set to 0')

                status_code, action, data = self.get_client_update(client)

                assert 'action' in data
                assert Action[action] == Action.UPDATE_TOKEN

                update_token = UpdateToken(**data)

                new_token = update_token.token

                assert status_code == 200
                assert Action[action] == Action.UPDATE_TOKEN

                # extend expire token
                session.execute(
                    update(ClientToken)
                    .where(ClientToken.client_id == client_id)
                    .values(expiration_time=86400)
                )
                session.commit()

                LOGGER.info('expiration_time for token set to 24h')

                status_code, _, _ = self.get_client_update(client)

                assert status_code == 403

                status_code, action, data = self.get_client_update(client, token=new_token)

                assert status_code == 200
                assert 'action' in data
                assert Action[action] == Action.DO_NOTHING
                assert len(data) == 1

    def test_client_update_app(self):
        """This will test the upload of a new (fake) app, and the update process."""
        with TestClient(api) as client:
            client_id = self.get_client(client)

            with Session(self.engine) as session:
                client_db = get_client_by_id(session, client_id)

                assert client_db is not None

                client_version = client_db.version

                assert client_version == 'test'

                # create fake app (it's just a file)
                version_app = 'test_1.0'
                filename_app = 'fake_client_app.json'
                path_fake_app = os.path.join('.', filename_app)

                with open(path_fake_app, 'w') as f:
                    json.dump({'version': version_app}, f)

                LOGGER.info(f'created file={path_fake_app}')

                # upload fake client app
                upload_response = client.post(
                    '/manager/upload/client',
                    files={
                        "file": (filename_app, open(path_fake_app, 'rb'))
                    }
                )

                assert upload_response.status_code == 200

                upload_id = upload_response.json()['upload_id']

                # update metadata
                metadata_response = client.post(
                    '/manager/upload/client/metadata',
                    json={
                        'upload_id': upload_id,
                        'version': version_app,
                        'name': 'Testing_app',
                        'active': True,
                    }
                )

                assert metadata_response.status_code == 200

                res = session.execute(select(ClientApp).where(ClientApp.app_id == upload_id))
                client_app: ClientApp | None = res.scalar_one_or_none()

                assert client_app is not None
                assert client_app.version == version_app
                assert client_app.active

                n_apps: int = session.scalar(select(func.count()).select_from(ClientApp).where(ClientApp.active))
                assert n_apps == 1

                newest_version: ClientApp | None = session.execute(
                    select(ClientApp)
                    .where(ClientApp.active)
                    .order_by(ClientApp.creation_time.desc())
                    .limit(1)
                ).scalar_one_or_none()

                assert newest_version is not None
                assert newest_version.version == version_app

                # update request
                status_code, action, data = self.get_client_update(client)

                assert status_code == 200
                assert Action[action] == Action.UPDATE_CLIENT

                update_client_app = UpdateClientApp(**data)

                assert update_client_app.version == version_app

                download_app = DownloadApp(name=update_client_app.name, version=update_client_app.version)

                assert self.server_key is not None

                # download new client
                with client.get(
                    '/client/download/application',
                    data=create_payload(self.server_key, json.dumps(download_app.dict())),
                    headers=headers(self.token),
                    stream=True,
                ) as stream:
                    assert stream.status_code == 200

                    content, checksum = decrypt_stream_response(stream, self.private_key)

                    assert update_client_app.checksum == checksum
                    assert json.loads(content) == {'version': version_app}

                client_db = get_client_by_id(session, client_id)
                session.refresh(client_db)

                assert client_db is not None
                assert client_db.version == version_app

                # delete local fake client app
                if os.path.exists(path_fake_app):
                    os.remove(path_fake_app)

                session.execute(
                    delete(ClientApp)
                    .where(ClientApp.app_id == upload_id)
                )
                session.commit()

    def test_update_metadata(self):
        with TestClient(api) as client:
            client_id = self.get_client(client)

            assert client_id is not None
            assert self.token is not None
            assert self.server_key is not None

            metadata: Metadata = get_metadata()
            upload_response: Response = send_metadata(client, self.token, self.server_key, metadata)

            assert upload_response.status_code == 200

            with Session(self.engine) as session:
                ds_db: ClientDataSource = session.execute(
                    select(ClientDataSource)
                    .where(ClientDataSource.client_id == client_id)
                ).scalar_one()

                assert ds_db.name == metadata.datasources[0].name
                assert ds_db.removed == metadata.datasources[0].removed
                assert ds_db.n_records == metadata.datasources[0].n_records
                assert ds_db.n_features == metadata.datasources[0].n_features

                ds_features = metadata.datasources[0].features

                ds_fs: list[ClientFeature] = session.scalars(
                    select(ClientFeature)
                    .where(ClientFeature.datasource_id == ds_db.datasource_id)
                ).all()

                assert len(ds_fs) == 2
                assert ds_fs[0].name == ds_features[0].name
                assert ds_fs[1].name == ds_features[1].name

    def test_task_get(self):
        with TestClient(api) as client:
            client_id = self.get_client(client)
            assert client_id is not None
            assert self.token is not None
            assert self.server_key is not None

            res = client.get('/workbench/connect')
            res.raise_for_status()
            wb_token = WorkbenchJoinData(**res.json()).token

            with Session(self.engine) as session:
                LOGGER.info('setup metadata for client')

                metadata: Metadata = get_metadata()
                ds_name = metadata.datasources[0].name
                upload_response: Response = send_metadata(client, self.token, self.server_key, metadata)

                assert upload_response.status_code == 200

                LOGGER.info('setup artifact')

                ds_list: list[ClientDataSource] = session.scalars(
                    select(ClientDataSource)
                ).all()

                assert client_id in [ds.client_id for ds in ds_list]

                ds = [ds for ds in ds_list if ds.name == ds_name][0]

                f1, f2 = session.scalars(
                    select(ClientFeature)
                    .where(
                        ClientFeature.datasource_id == ds.datasource_id,
                        ClientFeature.removed == False
                    )
                ).all()

                qf1 = QueryFeature(
                    feature_id=f1.feature_id,
                    feature_name=f1.name,
                    datasource_id=f1.datasource_id,
                    datasource_name=f1.datasource_name,
                )
                qf2 = QueryFeature(
                    feature_id=f2.feature_id,
                    feature_name=f2.name,
                    datasource_id=f2.datasource_id,
                    datasource_name=f2.datasource_name,
                )

                artifact = Artifact(
                    artifact_id=None,
                    dataset=Dataset(
                        queries=[Query(
                            datasource_id=ds.datasource_id,
                            datasource_name=ds.name,
                            features=[qf1, qf2],
                            filters=[QueryFilter(feature=qf1, operation=Operations.NUM_LESS_THAN.name, parameter='1.0')],
                            transformers=[],
                        )],
                    ),
                    model=Model(name=''),
                )

                LOGGER.info('submit artifact')

                submit_response = client.post(
                    '/workbench/artifact/submit',
                    json=artifact.dict(),
                    headers=headers(wb_token),
                )

                assert submit_response.status_code == 200

                artifact_status = ArtifactStatus(**submit_response.json())

            with Session(self.engine) as session:
                n = session.scalar(select(func.count()).select_from(Job))
                assert n == 1

                n = session.scalar(select(func.count()).select_from(Job).where(Job.client_id == client_id))
                assert n == 1

                job: Job = session.scalar(select(Job).limit(1))

                LOGGER.info('update client')

                status_code, action, data = self.get_client_update(client)

                assert status_code == 200
                assert Action[action] == Action.EXECUTE

                update_execute = UpdateExecute(**data)

                assert update_execute.artifact_id == job.artifact_id

                LOGGER.info('get task for client')

                with client.get(
                    '/client/task/',
                    data=create_payload(self.server_key, json.dumps(update_execute.dict())),
                    headers=headers(self.token),
                    stream=True,
                ) as task_response:
                    assert task_response.status_code == 200

                    content = get_payload(self.private_key, task_response.content)

                    assert 'artifact_id' in content
                    assert 'model' in content
                    assert 'dataset' in content

                    task = Artifact(**content)

                    assert task.artifact_id == job.artifact_id
                    assert artifact_status.artifact_id is not None
                    assert task.artifact_id == artifact_status.artifact_id
                    assert len(task.dataset.queries) == 1
                    assert len(task.dataset.queries[0].features) == 2

                # cleanup
                LOGGER.info('cleaning up')
                shutil.rmtree(os.path.join(STORAGE_ARTIFACTS, artifact_status.artifact_id))

                session.execute(delete(Job))
                session.commit()
