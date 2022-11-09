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
    WorkbenchDataSourceIdList,
)
from ferdelance_shared.models import Model
from ferdelance_shared.status import ArtifactJobStatus
from ferdelance_shared.exchange import Exchange

from .utils import (
    setup_test_database,
    create_client,
    get_metadata,
    send_metadata,
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
        self.cl_exc = Exchange()
        self.cl_exc.generate_key()

        # this is for workbench
        self.wb_exc = Exchange()
        self.wb_exc.generate_key()

        random.seed(42)

        LOGGER.info('setup completed')

    def connect(self, server: TestClient) -> tuple[str, str]:
        # this is to have a client
        client_id = create_client(server, self.cl_exc)

        metadata: Metadata = get_metadata()
        upload_response: Response = send_metadata(server, self.cl_exc, metadata)

        assert upload_response.status_code == 200

        # this is for connect a new workbench
        wjr = WorkbenchJoinRequest(
            public_key=self.wb_exc.transfer_public_key()
        )

        res = server.post(
            '/workbench/connect',
            data=json.dumps(wjr.dict())
        )

        res.raise_for_status()

        wjd = WorkbenchJoinData(**self.wb_exc.get_payload(res.content))

        self.wb_exc.set_remote_key(wjd.public_key)
        self.wb_exc.set_token(wjd.token)

        return client_id, wjd.id

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
                headers=self.wb_exc.headers(),
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
                headers=self.wb_exc.headers(),
            )

            res.raise_for_status()

            wcl = WorkbenchClientList(**self.wb_exc.get_payload(res.content))
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
                headers=self.wb_exc.headers(),
            )

            assert res.status_code == 200

            cd = ClientDetails(**self.wb_exc.get_payload(res.content))

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
                headers=self.wb_exc.headers(),
            )

            assert res.status_code == 200

            wdsl = WorkbenchDataSourceIdList(**self.wb_exc.get_payload(res.content))
            ds_list = wdsl.datasource_ids

            assert len(ds_list) == 1

            datasource_id = ds_list[0]

            res = server.get(
                f'/workbench/datasource/{datasource_id}',
                headers=self.wb_exc.headers(),
            )

            assert res.status_code == 200

            datasource: DataSource = DataSource(**self.wb_exc.get_payload(res.content))

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
                data=self.wb_exc.create_payload(artifact.dict()),
                headers=self.wb_exc.headers(),
            )

            assert res.status_code == 200

            status = ArtifactStatus(**self.wb_exc.get_payload(res.content))

            artifact_id = status.artifact_id

            assert status.status is not None
            assert artifact_id is not None
            assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

            res = server.get(
                f'/workbench/artifact/status/{artifact_id}',
                headers=self.wb_exc.headers(),
            )

            assert res.status_code == 200

            status = ArtifactStatus(**self.wb_exc.get_payload(res.content))
            assert status.status is not None
            assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

            res = server.get(
                f'/workbench/artifact/{artifact_id}',
                headers=self.wb_exc.headers(),
            )

            assert res.status_code == 200

            downloaded_artifact = Artifact(**self.wb_exc.get_payload(res.content))

            assert downloaded_artifact.artifact_id is not None
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
