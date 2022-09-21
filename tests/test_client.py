from base64 import b64encode

from ferdelance.database import SessionLocal
from ferdelance.database.settings import KeyValueStore
from ferdelance.database.tables import Client, ClientApp, ClientDataSource, ClientEvent, ClientFeature, ClientToken, ClientTask
from ferdelance.server.schemas.workbench import ArtifactSubmitRequest, QueryRequest, ModelRequest, StrategyRequest, QueryFeature, QueryFilter
from ferdelance.server.security import PUBLIC_KEY
from ferdelance.server.services.application import ClientAppService
from ferdelance.server.services.client import ClientService
from ferdelance.server.services.datasource import DataSourceService
from ferdelance.server.folders import STORAGE_ARTIFACTS

from ferdelance_shared.actions import EXEC

from .utils import (
    setup_test_client,
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

from requests import Response

import json
import logging
import os
import random

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

        self.client = setup_test_client()

        self.db_string, self.db_string_no_db = setup_test_database()

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

        teardown_test_database(self.db_string_no_db)

        LOGGER.info('teardown completed')

    def get_client(self):
        client_id, self.token, self.server_key = create_client(self.client, self.private_key)
        return client_id

    def get_client_update(self, payload, token: str | None = None) -> tuple[int, str, str]:
        payload = create_payload(self.server_key, payload)

        cur_token = token if token else self.token

        response = self.client.get(
            '/client/update',
            json=payload,
            headers=headers(cur_token)
        )

        if response.status_code != 200:
            return response.status_code, None, None

        response_payload = get_payload(self.private_key, response.json())

        assert 'action' in response_payload
        assert 'data' in response_payload

        return response.status_code, response_payload['action'], response_payload['data']

    def test_read_home(self):
        """Generic test to check if the home works."""
        response = self.client.get('/')

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

        client_id = self.get_client()

        with SessionLocal() as db:
            cs: ClientService = ClientService(db)

            db_client = cs.get_client_by_id(client_id)
            db_token = cs.get_client_token_by_client_id(client_id)

            assert db_client is not None
            assert db_client.active
            assert db_token is not None
            assert db_token.token == self.token
            assert db_token.valid

            kvs = KeyValueStore(db)
            server_public_key_db: str = kvs.get_str(PUBLIC_KEY)

            assert server_public_key_db == bytes_from_public_key(self.server_key).decode('utf8')

            db_events: list[ClientEvent] = cs.get_all_client_events(db_client)

            assert len(db_events) == 1
            assert db_events[0].event == 'creation'

    def test_client_already_exists(self):
        """This test will send twice the access information and expect the second time to receive a 403 error."""

        client_id = self.get_client()
        public_key = bytes_from_public_key(self.public_key)

        with SessionLocal() as db:
            cs: ClientService = ClientService(db)

            client = cs.get_client_by_id(client_id)

            data = {
                'system': client.machine_system,
                'mac_address': client.machine_mac_address,
                'node': client.machine_node,
                'public_key': b64encode(public_key).decode('utf8'),
                'version': 'test'
            }

        response = self.client.post(
            '/client/join',
            json=data
        )

        assert response.status_code == 403
        assert response.json()['detail'] == 'Invalid client data'

    def test_client_update(self):
        """This will test the endpoint for updates."""

        client_id = self.get_client()

        status_code, action, data = self.get_client_update({})

        assert status_code == 200
        assert action == 'nothing'
        assert not data

        with SessionLocal() as db:
            cs: ClientService = ClientService(db)

            db_client: Client = cs.get_client_by_id(client_id)

            assert db_client is not None

            db_events: list[str] = [c.event for c in cs.get_all_client_events(db_client)]

            assert len(db_events) == 3
            assert 'creation' in db_events
            assert 'update' in db_events
            assert 'action:nothing' in db_events

    def test_client_leave(self):
        """This will test the endpoint for leave a client."""
        client_id = self.get_client()

        response_leave = self.client.post(
            '/client/leave',
            json={},
            headers=headers(self.token)
        )

        LOGGER.info(f'response_leave={response_leave}')

        assert response_leave.status_code == 200

        # cannot get other updates
        status_code, _, _ = self.get_client_update({})

        assert status_code == 403

        with SessionLocal() as db:
            cs: ClientService = ClientService(db)

            db_client: Client = cs.get_client_by_id(client_id)

            assert db_client is not None
            assert db_client.active is False
            assert db_client.left

            db_events: list[str] = [c.event for c in cs.get_all_client_events(db_client)]

            assert 'creation' in db_events
            assert 'left' in db_events
            assert 'update' not in db_events

    def test_client_update_token(self):
        """This will test the failure and update of a token."""
        with SessionLocal() as db:
            client_id = self.get_client()

            # expire token
            db.query(ClientToken).filter(ClientToken.client_id == client_id).update({'expiration_time': 0})
            db.commit()

            LOGGER.info('expiration_time for token set to 0')

            status_code, action, data = self.get_client_update({})
            new_token = data['token']

            assert status_code == 200
            assert action == 'update_token'

            # extend expire token
            db.query(ClientToken).filter(ClientToken.client_id == client_id).update({'expiration_time': 86400})
            db.commit()

            LOGGER.info('expiration_time for token set to 24h')

            status_code, _, _ = self.get_client_update({})

            assert status_code == 403

            status_code, action, data = self.get_client_update({}, new_token)

            assert status_code == 200
            assert action == 'nothing'
            assert not data

    def test_client_update_app(self):
        """This will test the upload of a new (fake) app, and the update process."""
        with SessionLocal() as db:
            cas: ClientAppService = ClientAppService(db)
            cs: ClientService = ClientService(db)

            client_id = self.get_client()

            client_version = cs.get_client_by_id(client_id).version

            assert client_version == 'test'

            # create fake app (it's just a file)
            version_app = 'test_1.0'
            filename_app = 'fake_client_app.json'
            path_fake_app = os.path.join('.', filename_app)

            with open(path_fake_app, 'w') as f:
                json.dump({'version': version_app}, f)

            LOGGER.info(f'created file={path_fake_app}')

            # upload fake client app
            upload_response = self.client.post(
                '/manager/upload/client',
                files={
                    "file": (filename_app, open(path_fake_app, 'rb'))
                }
            )

            assert upload_response.status_code == 200

            upload_id = upload_response.json()['upload_id']

            # update metadata
            metadata_response = self.client.post(
                '/manager/upload/client/metadata',
                json={
                    'upload_id': upload_id,
                    'version': version_app,
                    'name': 'Testing_app',
                    'active': True,
                }
            )

            assert metadata_response.status_code == 200

            client_app: ClientApp = db.query(ClientApp).filter(ClientApp.app_id == upload_id).first()

            assert client_app is not None
            assert client_app.version == version_app
            assert client_app.active

            n_apps = db.query(ClientApp).filter(ClientApp.active).count()
            assert n_apps == 1

            newest_version: ClientApp = cas.get_newest_app_version()
            assert newest_version.version == version_app

            # update request
            status_code, action, data = self.get_client_update({})

            assert status_code == 200

            assert action == 'update_client'
            assert data['version'] == version_app

            # download new client
            with self.client.get(
                '/client/update/files',
                json=create_payload(self.server_key, {'client_version': data['version']}),
                headers=headers(self.token),
                stream=True,
            ) as stream:
                assert stream.status_code == 200

                content, checksum = decrypt_stream_response(stream, self.private_key)

                assert data['checksum'] == checksum
                assert json.loads(content) == {'version': version_app}

            client_version = cs.get_client_by_id(client_id).version

            assert client_version == version_app

            # delete local fake client app
            if os.path.exists(path_fake_app):
                os.remove(path_fake_app)

            db.query(ClientApp).filter(ClientApp.app_id == upload_id).delete()
            db.commit()

    def test_update_metadata(self):
        with SessionLocal() as db:
            client_id = self.get_client()

            ds_content: dict = get_metadata()
            upload_response: Response = send_metadata(self.client, self.token, self.server_key, ds_content)

            assert upload_response.status_code == 200

            ds_db: ClientDataSource = db.query(ClientDataSource).filter(ClientDataSource.client_id == client_id).first()

            assert ds_db is not None
            assert ds_db.name == ds_content['datasources'][0]['name']
            assert ds_db.type == ds_content['datasources'][0]['type']
            assert ds_db.removed == ds_content['datasources'][0]['removed']
            assert ds_db.n_records == ds_content['datasources'][0]['n_records']
            assert ds_db.n_features == ds_content['datasources'][0]['n_features']

            ds_features = ds_content['datasources'][0]['features']

            ds_fs: list[ClientFeature] = db.query(ClientFeature).filter(ClientFeature.datasource_id == ds_db.datasource_id).all()

            assert len(ds_fs) == 2
            assert ds_fs[0].name == ds_features[0]['name']
            assert ds_fs[1].name == ds_features[1]['name']

    def test_task_get(self):
        with SessionLocal() as db:
            client_id = self.get_client()
            dss: DataSourceService = DataSourceService(db)

            # setup metadata for client
            ds_content: dict = get_metadata()
            ds_name = ds_content['datasources'][0]['name']
            upload_response: Response = send_metadata(self.client, self.token, self.server_key, ds_content)

            assert upload_response.status_code == 200

            # setup artifact
            ds_list = dss.get_datasource_list()
            ds = [ds for ds in ds_list if ds.name == ds_name][0]

            f1, f2 = dss.get_features_by_datasource(ds)

            qf1 = QueryFeature(feature_id=f1.feature_id, datasource_id=f1.datasource_id)
            qf2 = QueryFeature(feature_id=f2.feature_id, datasource_id=f2.datasource_id)

            artifact = ArtifactSubmitRequest(
                query=QueryRequest(
                    datasources=[ds.datasource_id],
                    features=[qf1, qf2],
                    filters=[QueryFilter(feature=qf1, operation='', parameter='')],
                    transformers=[],
                ),
                model=ModelRequest(name=''),
                strategy=StrategyRequest(strategy='')
            )

            # submit artifact
            submit_response = self.client.post(
                '/workbench/artifact/submit',
                json=artifact.dict(),
                headers=headers(self.token),
            )

            assert submit_response.status_code == 200

            artifact_id: str = submit_response.json()['artifact_id']

            n = db.query(ClientTask).filter(ClientTask.client_id == client_id).count()
            assert n == 1

            # update client
            status_code, action, data = self.get_client_update({})

            assert status_code == 200
            assert action == EXEC
            assert 'client_task_id' in data

            client_task_id: str = data['client_task_id']

            # get task for client
            with self.client.get(
                f'/client/task/',
                json=create_payload(self.server_key, {'client_task_id': client_task_id}),
                headers=headers(self.token),
                stream=True,
            ) as task_response:
                assert task_response.status_code == 200

                content, _ = decrypt_stream_response(task_response, self.private_key)

                content = json.loads(content)

                assert 'artifact_id' in content
                assert content['artifact_id'] == artifact_id
                assert len(content['features']) == 2

            # cleanup
            os.remove(os.path.join(STORAGE_ARTIFACTS, f'{artifact_id}.json'))

            db.query(ClientTask).filter(ClientTask.task_id == client_task_id).delete()
            db.commit()
