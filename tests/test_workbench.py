from ferdelance.config import STORAGE_ARTIFACTS
from ferdelance.server.api import api

from ferdelance_shared.schemas import (
    Artifact,
    ArtifactStatus,
    ClientDetails,
    Dataset,
    DataSource,
    Metadata,
    Query,
    QueryFeature,
    WorkbenchJoinRequest,
    WorkbenchJoinData,
    WorkbenchClientList,
    WorkbenchClientDataSourceList,
)
from ferdelance_shared.models import Model
from ferdelance_shared.status import ArtifactJobStatus

from .utils import (
    headers,
    setup_test_database,
    setup_rsa_keys,
    create_client,
    bytes_from_public_key,
    get_metadata,
    send_metadata,
    get_payload,
    create_payload,
)
from .crud import (
    get_user_by_id,
    get_client_by_id,
    delete_artifact,
    delete_client,
    Session,
    delete_datasource,
    delete_job,
    delete_user,
)

from fastapi.testclient import TestClient

from requests import Response

from base64 import b64encode

import json
import logging
import os
import random
import shutil

LOGGER = logging.getLogger(__name__)


class TestWorkbenchClass:

    def setup_class(self):
        """Class setup. This will be executed once each test. The setup will:
        - Create a new database on the remote server specified by `DB_HOST`, `DB_USER`, and `DB_PASS` (all env variables.).
            The name of the database is randomly generated using UUID4, if not supplied via `DB_SCHEMA` env variable.
            The database will be used as the server's database.
        - Populate this database with the required tables.
        - Generate and save to the database the servers' keys using the hardcoded `SERVER_MAIN_PASSWORD`.
        - Generate the local public/private keys to simulate a client application.
        """
        LOGGER.info('setting up')

        self.engine = setup_test_database()

        # this is for client
        self.client_private_key = setup_rsa_keys()
        self.client_public_key = self.client_private_key.public_key()

        # this is for workbench
        self.wb_private_key = setup_rsa_keys()
        self.wb_public_key = self.wb_private_key.public_key()

        random.seed(42)

        LOGGER.info('setup completed')

    def connect(self, server: TestClient) -> tuple[str, str]:
        # this is to have a client
        client_id, token, self.server_key = create_client(server, self.client_private_key)

        metadata: Metadata = get_metadata()
        upload_response: Response = send_metadata(server, token, self.server_key, metadata)

        assert upload_response.status_code == 200

        # this is for connect a new workbench
        public_key = bytes_from_public_key(self.wb_public_key)

        wjr = WorkbenchJoinRequest(
            public_key=b64encode(public_key).decode('utf8')
        )

        res = server.post(
            '/workbench/connect',
            data=json.dumps(wjr.dict())
        )

        res.raise_for_status()

        wjd = WorkbenchJoinData(**get_payload(self.wb_private_key, res.content))
        self.wb_token = wjd.token
        wb_id = wjd.id

        return client_id, wb_id

    def test_workbench_connect(self):
        with TestClient(api) as server:
            client_id, wb_id = self.connect(server)

            assert client_id is not None
            assert wb_id is not None

            with Session(self.engine) as session:
                uid = get_user_by_id(session, wb_id)
                cid = get_client_by_id(session, client_id)

                assert uid is not None
                assert cid is not None

                delete_datasource(session, client_id=client_id)
                delete_client(session, client_id)
                delete_user(session, wb_id)

    def test_workbench_read_home(self):
        """Generic test to check if the home works."""
        with TestClient(api) as server:
            client_id, wb_id = self.connect(server)

            res = server.get(
                '/workbench',
                headers=headers(self.wb_token)
            )

            assert res.status_code == 200
            assert res.content.decode('utf8') == '"Workbench 🔧"'

            with Session(self.engine) as session:
                delete_datasource(session, client_id=client_id)
                delete_client(session, client_id)
                delete_user(session, wb_id)

    def test_workbench_list_client(self):
        with TestClient(api) as server:
            client_id, wb_id = self.connect(server)

            res = server.get(
                '/workbench/client/list',
                headers=headers(self.wb_token)
            )

            res.raise_for_status()

            wcl = WorkbenchClientList(**get_payload(self.wb_private_key, res.content))
            client_list = wcl.client_ids

            assert len(client_list) == 1
            assert 'SERVER' not in client_list
            assert 'WORKER' not in client_list
            assert 'WORKBENCH' not in client_list

            with Session(self.engine) as session:
                delete_datasource(session, client_id=client_id)
                delete_client(session, client_id)
                delete_user(session, wb_id)

    def test_workbench_detail_client(self):
        with TestClient(api) as server:
            client_id, wb_id = self.connect(server)

            res = server.get(
                f'/workbench/client/{client_id}',
                headers=headers(self.wb_token)
            )

            assert res.status_code == 200

            cd = ClientDetails(**get_payload(self.wb_private_key, res.content))

            assert cd.client_id == client_id
            assert cd.version == 'test'

            with Session(self.engine) as session:
                delete_datasource(session, client_id=client_id)
                delete_client(session, client_id)
                delete_user(session, wb_id)

    def test_workflow_submit(self):
        with TestClient(api) as server:
            client_id, wb_id = self.connect(server)

            res = server.get(
                '/workbench/datasource/list',
                headers=headers(self.wb_token)
            )

            assert res.status_code == 200

            wcdsl = WorkbenchClientDataSourceList(**get_payload(self.wb_private_key, res.content))
            ds_list = wcdsl.datasource_ids

            assert len(ds_list) == 1

            datasource_id = ds_list[0]

            res = server.get(
                f'/workbench/datasource/{datasource_id}',
                headers=headers(self.wb_token)
            )

            assert res.status_code == 200

            datasource: DataSource = DataSource(**get_payload(self.wb_private_key, res.content))

            assert len(datasource.features) == 2
            assert datasource.n_records == 1000
            assert datasource.n_features == 2

            assert len(datasource.features) == datasource.n_features

            dtypes = [f.dtype for f in datasource.features]

            assert 'float' in dtypes
            assert 'int' in dtypes

            artifact = Artifact(
                artifact_id=None,
                dataset=Dataset(
                    queries=[
                        Query(
                            datasource_id=datasource.datasource_id,
                            datasource_name=datasource.name,
                            features=[QueryFeature(
                                datasource_id=f.datasource_id,
                                datasource_name=f.datasource_name,
                                feature_id=f.feature_id,
                                feature_name=f.name,
                            ) for f in datasource.features]
                        )
                    ]
                ),
                model=Model(name='model', strategy=''),
            )

            res = server.post(
                '/workbench/artifact/submit',
                data=create_payload(self.server_key, json.dumps(artifact.dict())),
                headers=headers(self.wb_token),
            )

            assert res.status_code == 200

            status = ArtifactStatus(**get_payload(self.wb_private_key, res.content))

            artifact_id = status.artifact_id

            assert status.status is not None
            assert artifact_id is not None
            assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

            res = server.get(f'/workbench/artifact/status/{artifact_id}', headers=headers(self.wb_token))

            assert res.status_code == 200

            status = ArtifactStatus(**get_payload(self.wb_private_key, res.content))
            assert status.status is not None
            assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

            res = server.get(f'/workbench/artifact/{artifact_id}', headers=headers(self.wb_token))

            assert res.status_code == 200

            downloaded_artifact = Artifact(**get_payload(self.wb_private_key, res.content))

            assert len(downloaded_artifact.dataset.queries) == 1
            assert downloaded_artifact.dataset.queries[0].datasource_id == datasource_id
            assert len(downloaded_artifact.dataset.queries[0].features) == 2

            shutil.rmtree(os.path.join(STORAGE_ARTIFACTS, artifact_id))

            with Session(self.engine) as session:
                delete_job(session, client_id)
                delete_artifact(session, downloaded_artifact.artifact_id)
                delete_datasource(session, client_id=client_id)
                delete_client(session, client_id)
                delete_user(session, wb_id)
