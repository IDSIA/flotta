from ferdelance.database import SessionLocal
from ferdelance.database.services import ClientAppService, ClientService, DataSourceService
from ferdelance.database.settings import KeyValueStore
from ferdelance.database.tables import Client, ClientApp, ClientDataSource, ClientEvent, ClientFeature, ClientToken, Job
from ferdelance.server.security import PUBLIC_KEY
from ferdelance.config import STORAGE_ARTIFACTS

from ferdelance_shared.actions import Action
from ferdelance_shared.schemas import *
from ferdelance_shared.schemas.models import *
from ferdelance_shared.operations import NumericOperations

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

from base64 import b64encode
from requests import Response

import json
import logging
import os
import random
import time

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

    def get_client_update(self, payload: str | None = None, token: str | None = None) -> tuple[int, str, str]:
        cur_token = token if token else self.token

        if payload is None:
            payload = json.dumps(ClientUpdate(action=Action.DO_NOTHING.name).dict())

        response = self.client.get(
            '/client/update',
            data=create_payload(self.server_key, payload),
            headers=headers(cur_token)
        )

        if response.status_code != 200:
            return response.status_code, None, None

        response_payload = get_payload(self.private_key, response.content)

        assert 'action' in response_payload

        return response.status_code, response_payload['action'], response_payload

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
            cs = ClientService(db)
            kvs = KeyValueStore(db)

            db_client = cs.get_client_by_id(client_id)
            db_token = cs.get_client_token_by_client_id(client_id)

            assert db_client is not None
            assert db_client.active
            assert db_token is not None
            assert db_token.token == self.token
            assert db_token.valid

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

            data = ClientJoinRequest(
                system=client.machine_system,
                mac_address=client.machine_mac_address,
                node=client.machine_node,
                public_key=b64encode(public_key).decode('utf8'),
                version='test',
            )

        response = self.client.post(
            '/client/join',
            json=data.dict()
        )

        assert response.status_code == 403
        assert response.json()['detail'] == 'Invalid client data'

    def test_client_update(self):
        """This will test the endpoint for updates."""

        client_id = self.get_client()

        status_code, action, data = self.get_client_update()

        assert status_code == 200
        assert Action[action] == Action.DO_NOTHING

        with SessionLocal() as db:
            cs: ClientService = ClientService(db)

            db_client: Client = cs.get_client_by_id(client_id)

            assert db_client is not None

            db_events: list[str] = [c.event for c in cs.get_all_client_events(db_client)]

            assert len(db_events) == 3
            assert 'creation' in db_events
            assert 'update' in db_events
            assert f'action:{Action.DO_NOTHING.name}' in db_events

    def test_client_leave(self):
        """This will test the endpoint for leave a client."""
        client_id = self.get_client()

        response_leave = self.client.post(
            '/client/leave',
            headers=headers(self.token)
        )

        LOGGER.info(f'response_leave={response_leave}')

        assert response_leave.status_code == 200

        # cannot get other updates
        status_code, _, _ = self.get_client_update()

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

            status_code, action, data = self.get_client_update()

            assert 'action' in data
            assert Action[action] == Action.UPDATE_TOKEN

            update_token = UpdateToken(**data)

            new_token = update_token.token

            assert status_code == 200
            assert Action[action] == Action.UPDATE_TOKEN

            # extend expire token
            db.query(ClientToken).filter(ClientToken.client_id == client_id).update({'expiration_time': 86400})
            db.commit()

            LOGGER.info('expiration_time for token set to 24h')

            status_code, _, _ = self.get_client_update()

            assert status_code == 403

            status_code, action, data = self.get_client_update(token=new_token)

            assert status_code == 200
            assert 'action' in data
            assert Action[action] == Action.DO_NOTHING
            assert len(data) == 1

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
            status_code, action, data = self.get_client_update()

            assert status_code == 200
            assert Action[action] == Action.UPDATE_CLIENT

            update_client_app = UpdateClientApp(**data)

            assert update_client_app.version == version_app

            download_app = DownloadApp(name=update_client_app.name, version=update_client_app.version)

            # download new client
            with self.client.get(
                '/client/download/application',
                data=create_payload(self.server_key, json.dumps(download_app.dict())),
                headers=headers(self.token),
                stream=True,
            ) as stream:
                assert stream.status_code == 200

                content, checksum = decrypt_stream_response(stream, self.private_key)

                assert update_client_app.checksum == checksum
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

            metadata: Metadata = get_metadata()
            upload_response: Response = send_metadata(self.client, self.token, self.server_key, metadata)

            assert upload_response.status_code == 200

            ds_db: ClientDataSource = db.query(ClientDataSource).filter(ClientDataSource.client_id == client_id).first()

            assert ds_db is not None
            assert ds_db.name == metadata.datasources[0].name
            assert ds_db.removed == metadata.datasources[0].removed
            assert ds_db.n_records == metadata.datasources[0].n_records
            assert ds_db.n_features == metadata.datasources[0].n_features

            ds_features = metadata.datasources[0].features

            ds_fs: list[ClientFeature] = db.query(ClientFeature).filter(ClientFeature.datasource_id == ds_db.datasource_id).all()

            assert len(ds_fs) == 2
            assert ds_fs[0].name == ds_features[0].name
            assert ds_fs[1].name == ds_features[1].name

    def test_task_get(self):
        with SessionLocal() as db:
            client_id = self.get_client()
            dss: DataSourceService = DataSourceService(db)

            LOGGER.info('setup metadata for client')

            metadata: Metadata = get_metadata()
            ds_name = metadata.datasources[0].name
            upload_response: Response = send_metadata(self.client, self.token, self.server_key, metadata)

            assert upload_response.status_code == 200

            LOGGER.info('setup artifact')

            ds_list = dss.get_datasource_list()

            assert client_id in [ds.client_id for ds in ds_list]

            ds = [ds for ds in ds_list if ds.name == ds_name][0]

            f1, f2 = dss.get_features_by_datasource(ds)

            qf1 = QueryFeature(feature_id=f1.feature_id, datasource_id=f1.datasource_id)
            qf2 = QueryFeature(feature_id=f2.feature_id, datasource_id=f2.datasource_id)

            artifact = Artifact(
                artifact_id=None,
                dataset=Dataset(
                    queries=[Query(
                        datasources_id=ds.datasource_id,
                        features=[qf1, qf2],
                        filters=[QueryFilter(feature=qf1, operation=NumericOperations.LESS_THAN.name, parameter='1.0')],
                        transformers=[],
                    )],
                ),
                model=Model(name='', strategy=None),
            )

            LOGGER.info('submit artifact')

            submit_response = self.client.post(
                '/workbench/artifact/submit',
                json=artifact.dict(),
                headers=headers(self.token),
            )

            assert submit_response.status_code == 200

            artifact_status = ArtifactStatus(**submit_response.json())

            n = db.query(Job).count()
            assert n == 1

            n = db.query(Job).filter(Job.client_id == client_id).count()
            assert n == 1

            job: Job = db.query(Job).all()[0]

            LOGGER.info('update client')

            status_code, action, data = self.get_client_update()

            assert status_code == 200
            assert Action[action] == Action.EXECUTE

            update_execute = UpdateExecute(**data)

            assert update_execute.job_id == job.job_id

            LOGGER.info('get task for client')

            with self.client.get(
                '/client/task/',
                data=create_payload(self.server_key, json.dumps(update_execute.dict())),
                headers=headers(self.token),
                stream=True,
            ) as task_response:
                assert task_response.status_code == 200

                content = get_payload(self.private_key, task_response.content)

                assert 'artifact_id' in content
                assert 'job_id' in content
                assert 'model' in content
                assert 'dataset' in content

                task = ArtifactTask(**content)

                assert task.artifact_id == job.artifact_id
                assert task.artifact_id == artifact_status.artifact_id
                assert task.job_id == job.job_id
                assert len(task.dataset.queries) == 1
                assert len(task.dataset.queries[0].features) == 2

            # cleanup
            LOGGER.info('cleaning up')
            os.remove(os.path.join(STORAGE_ARTIFACTS, f'{artifact_status.artifact_id}.json'))

            db.query(Job).delete()
            db.commit()
