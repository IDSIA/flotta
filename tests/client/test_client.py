from ferdelance.database.tables import (
    Client,
    ClientApp,
    ClientDataSource,
    ClientFeature,
    ClientToken,
    Job,
)
from ferdelance.server.api import api
from ferdelance.config import STORAGE_ARTIFACTS

from ferdelance.shared.actions import Action
from ferdelance.shared.artifacts import (
    Artifact,
    ArtifactStatus,
    Dataset,
    Metadata,
    Query,
    QueryFeature,
    QueryFilter,
)
from ferdelance.shared.schemas import (
    ClientUpdate,
    ClientJoinRequest,
    DownloadApp,
    UpdateClientApp,
    UpdateExecute,
    UpdateToken,
    WorkbenchJoinData,
    WorkbenchJoinRequest,
)
from ferdelance.shared.models import Model
from ferdelance.shared.artifacts.operations import Operations
from ferdelance.shared.exchange import Exchange

from ..server.utils import (
    setup_test_database,
    create_client,
    get_metadata,
    send_metadata,
)
from ..server.crud import (
    delete_client,
    get_client_by_id,
    get_token_by_id,
    get_client_events,
    delete_datasource,
    delete_artifact,
    delete_job,
)

from fastapi.testclient import TestClient

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

        self.exc = Exchange()
        self.exc.generate_key()

        random.seed(42)

        LOGGER.info('setup completed')

    def get_client_update(self, client: TestClient) -> tuple[int, str, Any]:
        payload = ClientUpdate(action=Action.DO_NOTHING.name)

        response = client.get(
            '/client/update',
            data=self.exc.create_payload(payload.dict()),
            headers=self.exc.headers()
        )

        if response.status_code != 200:
            return response.status_code, '', None

        response_payload = self.exc.get_payload(response.content)

        assert 'action' in response_payload

        return response.status_code, response_payload['action'], response_payload

    def test_client_read_home(self):
        """Generic test to check if the home works."""
        with TestClient(api) as client:
            client_id = create_client(client, self.exc)

            response = client.get('/')

            assert response.status_code == 200
            assert response.content.decode('utf8') == '"Hi! 😀"'

            with Session(self.engine) as session:
                delete_client(session, client_id)

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
            client_id = create_client(client, self.exc)

            with Session(self.engine) as session:
                db_client: Client | None = get_client_by_id(session, client_id)

                assert db_client is not None
                assert db_client.active

                db_token: ClientToken | None = get_token_by_id(session, client_id)

                assert db_token is not None
                assert db_token.token == self.exc.token
                assert db_token.valid

                db_events: list[str] = get_client_events(session, client_id)

                assert len(db_events) == 1
                assert db_events[0] == 'creation'

                delete_client(session, client_id)

    def test_client_already_exists(self):
        """This test will send twice the access information and expect the second time to receive a 403 error."""

        with TestClient(api) as client:
            client_id = create_client(client, self.exc)

            with Session(self.engine) as session:
                client_db = get_client_by_id(session, client_id)

                assert client_db is not None

                data = ClientJoinRequest(
                    system=client_db.machine_system,
                    mac_address=client_db.machine_mac_address,
                    node=client_db.machine_node,
                    public_key=self.exc.transfer_public_key(),
                    version='test',
                )

            response = client.post(
                '/client/join',
                json=data.dict(),
            )

            assert response.status_code == 403
            assert response.json()['detail'] == 'Invalid client data'

            delete_client(session, client_id)

    def test_client_update(self):
        """This will test the endpoint for updates."""

        with TestClient(api) as client:
            client_id = create_client(client, self.exc)

            status_code, action, _ = self.get_client_update(client)

            assert status_code == 200
            assert Action[action] == Action.DO_NOTHING

            with Session(self.engine) as session:
                db_events: list[str] = get_client_events(session, client_id)

                assert len(db_events) == 3
                assert 'creation' in db_events
                assert 'update' in db_events
                assert f'action:{Action.DO_NOTHING.name}' in db_events

            delete_client(session, client_id)

    def test_client_leave(self):
        """This will test the endpoint for leave a client."""
        with TestClient(api) as client:
            client_id = create_client(client, self.exc)

            response_leave = client.post(
                '/client/leave',
                headers=self.exc.headers()
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

            delete_client(session, client_id)

    def test_client_update_token(self):
        """This will test the failure and update of a token."""
        with TestClient(api) as client:
            client_id = create_client(client, self.exc)

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

                self.exc.set_token(new_token)
                status_code, action, data = self.get_client_update(client)

                assert status_code == 200
                assert 'action' in data
                assert Action[action] == Action.DO_NOTHING
                assert len(data) == 1

            delete_client(session, client_id)

    def test_client_update_app(self):
        """This will test the upload of a new (fake) app, and the update process."""
        with TestClient(api) as client:
            client_id = create_client(client, self.exc)

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

                assert self.exc.remote_key is not None

                # download new client
                with client.get(
                    '/client/download/application',
                    data=self.exc.create_payload(download_app.dict()),
                    headers=self.exc.headers(),
                    stream=True,
                ) as stream:
                    assert stream.status_code == 200

                    content, checksum = self.exc.stream_response(stream)

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

            delete_client(session, client_id)

    def test_update_metadata(self):
        with TestClient(api) as client:
            client_id = create_client(client, self.exc)

            assert client_id is not None

            metadata: Metadata = get_metadata()
            upload_response: Response = send_metadata(client, self.exc, metadata)

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

            delete_datasource(session, ds_db.datasource_id)
            delete_client(session, client_id)

    def test_client_task_get(self):
        with TestClient(api) as server:
            client_id = create_client(server, self.exc)

            with Session(self.engine) as session:
                LOGGER.info('setup metadata for client')

                metadata: Metadata = get_metadata()
                upload_response: Response = send_metadata(server, self.exc, metadata)

                assert upload_response.status_code == 200

                LOGGER.info('setup artifact')

                ds_db: ClientDataSource | None = session.execute(
                    select(ClientDataSource)
                    .where(ClientDataSource.client_id == client_id)
                ).scalar_one_or_none()

                assert ds_db is not None

                fs = session.scalars(
                    select(ClientFeature)
                    .where(
                        ClientFeature.datasource_id == ds_db.datasource_id,
                        ClientFeature.removed == False
                    )
                ).all()

                assert len(fs) == 2

                f1: ClientFeature = fs[0]
                f2: ClientFeature = fs[1]

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
                            datasource_id=ds_db.datasource_id,
                            datasource_name=ds_db.name,
                            features=[qf1, qf2],
                            filters=[QueryFilter(feature=qf1, operation=Operations.NUM_LESS_THAN.name, parameter='1.0')],
                            transformers=[],
                        )],
                    ),
                    model=Model(name=''),
                )

                LOGGER.info('submit artifact')

                wb_exc = Exchange()
                wb_exc.generate_key()

                wjr = WorkbenchJoinRequest(
                    public_key=wb_exc.transfer_public_key()
                )

                res = server.post(
                    '/workbench/connect',
                    data=json.dumps(wjr.dict())
                )

                res.raise_for_status()

                wjd = WorkbenchJoinData(**wb_exc.get_payload(res.content))

                wb_exc.set_remote_key(wjd.public_key)
                wb_exc.set_token(wjd.token)

                submit_response = server.post(
                    '/workbench/artifact/submit',
                    data=wb_exc.create_payload(artifact.dict()),
                    headers=wb_exc.headers(),
                )

                assert submit_response.status_code == 200

                artifact_status = ArtifactStatus(**wb_exc.get_payload(submit_response.content))

                n = session.scalar(select(func.count()).select_from(Job))
                assert n == 1

                n = session.scalar(select(func.count()).select_from(Job).where(Job.client_id == client_id))
                assert n == 1

                job: Job = session.scalar(select(Job).limit(1))

                LOGGER.info('update client')

                status_code, action, data = self.get_client_update(server)

                assert status_code == 200
                assert Action[action] == Action.EXECUTE

                update_execute = UpdateExecute(**data)

                assert update_execute.artifact_id == job.artifact_id

                LOGGER.info('get task for client')

                with server.get(
                    '/client/task/',
                    data=self.exc.create_payload(update_execute.dict()),
                    headers=self.exc.headers(),
                    stream=True,
                ) as task_response:
                    assert task_response.status_code == 200

                    content = self.exc.get_payload(task_response.content)

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

                delete_job(session, job.client_id)
                delete_artifact(session, artifact_status.artifact_id)
                delete_datasource(session, ds_db.datasource_id)
                delete_client(session, client_id)
