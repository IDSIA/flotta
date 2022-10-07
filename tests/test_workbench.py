from ferdelance.config import STORAGE_ARTIFACTS

from ferdelance_shared.schemas import *
from ferdelance_shared.schemas.models import *
from ferdelance_shared.status import ArtifactJobStatus

from .utils import (
    setup_test_client,
    setup_test_database,
    setup_rsa_keys,
    teardown_test_database,
    create_client,
    bytes_from_public_key,
    get_metadata,
    send_metadata,
)

from requests import Response

import json
import logging
import os
import random

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

        self.client = setup_test_client()

        self.db_string, self.db_string_no_db = setup_test_database()

        self.private_key = setup_rsa_keys()
        self.public_key = self.private_key.public_key()
        self.public_key_bytes = bytes_from_public_key(self.public_key)

        random.seed(42)

        self.server_key = None
        self.token = None

        self.client_id, self.token, self.server_key = create_client(self.client, self.private_key)

        metadata: Metadata = get_metadata()
        upload_response: Response = send_metadata(self.client, self.token, self.server_key, metadata)

        assert upload_response.status_code == 200

        LOGGER.info('setup completed')

    def teardown_class(self):
        """Class teardown. This method will ensure that the database is closed and deleted from the remote dbms.
        Note that all database connections still open will be forced to close by this method.
        """
        LOGGER.info('tearing down')

        teardown_test_database(self.db_string_no_db)

        LOGGER.info('teardown completed')

    def test_read_workbench_home(self):
        """Generic test to check if the home works."""
        response = self.client.get('/workbench')

        assert response.status_code == 200
        assert response.content.decode('utf8') == '"Workbench 🔧"'

    def test_client_list(self):
        res = self.client.get('/workbench/client/list')

        assert res.status_code == 200

        client_list = json.loads(res.content)

        assert len(client_list) == 1
        assert 'SERVER' not in client_list
        assert 'WORKER' not in client_list
        assert 'WORKBENCH' not in client_list

    def test_client_detail(self):
        client_id = self.client_id

        res = self.client.get(f'/workbench/client/{client_id}')

        assert res.status_code == 200

        cd = ClientDetails(**json.loads(res.content))

        assert cd.client_id == client_id
        assert cd.version == 'test'

    def test_workflow_submit(self):
        res = self.client.get('/workbench/datasource/list')

        assert res.status_code == 200

        ds_list = json.loads(res.content)

        assert len(ds_list) == 1

        datasource_id = ds_list[0]

        res = self.client.get(f'/workbench/datasource/{datasource_id}')

        assert res.status_code == 200

        datasource: DataSource = DataSource(**json.loads(res.content))

        assert len(datasource.features) == 2
        assert datasource.n_records == 1000
        assert datasource.n_features == 2

        assert len(datasource.features) == datasource.n_features

        dtypes = [f.dtype for f in datasource.features]

        assert 'float' in dtypes
        assert 'int' in dtypes

        artifact = Artifact(
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
            model=Model(name='model', strategy=None),
        )

        res = self.client.post(
            '/workbench/artifact/submit',
            json=artifact.dict(),
        )

        assert res.status_code == 200

        status = ArtifactStatus(**json.loads(res.content))

        artifact_id = status.artifact_id

        assert artifact_id is not None
        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        res = self.client.get(f'/workbench/artifact/status/{artifact_id}')

        assert res.status_code == 200

        status = ArtifactStatus(**json.loads(res.content))
        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        res = self.client.get(f'/workbench/artifact/{artifact_id}')

        assert res.status_code == 200

        downloaded_artifact = Artifact(**json.loads(res.content))

        assert len(downloaded_artifact.dataset.queries) == 1
        assert downloaded_artifact.dataset.queries[0].datasource_id == datasource_id
        assert len(downloaded_artifact.dataset.queries[0].features) == 2

        os.remove(os.path.join(STORAGE_ARTIFACTS, f'{artifact_id}.json'))
