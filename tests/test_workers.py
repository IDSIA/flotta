from ferdelance.database.tables import (
    Artifact as ArtifactDB,
    ClientDataSource,
    ClientFeature,
    ClientToken,
    Client,
    Model as ModelDB,
)
from ferdelance.server.api import api

from ferdelance_shared.schemas import (
    Artifact,
    ArtifactStatus,
    Metadata,
    Dataset,
    Query,
    QueryFeature,
)
from ferdelance_shared.models import Model
from ferdelance_shared.status import JobStatus

from .utils import (
    setup_test_database,
    setup_rsa_keys,
    teardown_test_database,
    bytes_from_public_key,
    create_client,
    get_metadata,
    send_metadata,
    headers,
)

from fastapi.testclient import TestClient
from requests import Response
from sqlalchemy import select, update, delete, func
from sqlalchemy.orm import Session

import logging
import os
import pickle
import uuid

LOGGER = logging.getLogger(__name__)


class TestFilesClass:

    def setup_class(self):
        LOGGER.info('setting up')

        self.engine = setup_test_database()

        self.private_key = setup_rsa_keys()
        self.public_key = self.private_key.public_key()
        self.public_key_bytes = bytes_from_public_key(self.public_key)

        self.server_key = None
        self.token = None

        with TestClient(api) as client:
            self.client_id, self.token, self.server_key = create_client(client, self.private_key)

            metadata: Metadata = get_metadata()
            upload_response: Response = send_metadata(client, self.token, self.server_key, metadata)

            assert upload_response.status_code == 200

    def teardown_class(self):
        """Class teardown. This method will ensure that the database is closed and deleted from the remote dbms.
        Note that all database connections still open will be forced to close by this method.
        """
        LOGGER.info('tearing down')

        teardown_test_database()

        LOGGER.info('teardown completed')

    def test_endpoints(self):
        with TestClient(api) as client:
            with Session(self.engine) as session:
                token: ClientToken | None = session.execute(
                    select(ClientToken.token)
                    .select_from(ClientToken)
                    .join(Client, Client.client_id == ClientToken.client_id)
                    .where(Client.type == 'WORKER')
                    .limit(1)
                ).scalar_one()

                assert token is not None
                assert isinstance(token, str)

                # test artifact not found
                res = client.get(
                    f'/worker/artifact/{uuid.uuid4()}',
                    headers=headers(token),
                )

                assert res.status_code == 404

                # prepare new artifact
                ds: ClientDataSource | None = session.query(ClientDataSource).first()

                assert ds is not None

                fs: list[ClientFeature] = session.query(ClientFeature).where(ClientFeature.datasource_id == ds.datasource_id).all()

                artifact = Artifact(
                    artifact_id=None,
                    dataset=Dataset(
                        queries=[
                            Query(
                                datasource_id=ds.datasource_id,
                                datasource_name=ds.name,
                                features=[QueryFeature(
                                    datasource_id=f.datasource_id,
                                    datasource_name=f.datasource_name,
                                    feature_id=f.feature_id,
                                    feature_name=f.name,
                                ) for f in fs]
                            )
                        ]
                    ),
                    model=Model(name='model', strategy=''),
                )

                # test artifact submit
                res = client.post(
                    '/worker/artifact',
                    headers=headers(token),
                    json=artifact.dict()
                )

                assert res.status_code == 200

                status: ArtifactStatus = ArtifactStatus(**res.json())

                LOGGER.info(f'artifact_id: {status.artifact_id}')

                artifact.artifact_id = status.artifact_id
                assert artifact.artifact_id is not None

                assert status.status is not None
                assert JobStatus[status.status] == JobStatus.SCHEDULED

                art_db: ArtifactDB | None = session.query(ArtifactDB).where(ArtifactDB.artifact_id == artifact.artifact_id).first()

                assert art_db is not None
                assert os.path.exists(art_db.path)

                # test artifact get
                res = client.get(
                    f'/worker/artifact/{status.artifact_id}',
                    headers=headers(token),
                )

                assert res.status_code == 200

                get_art: Artifact = Artifact(**res.json())

                assert artifact.artifact_id == get_art.artifact_id

                assert len(artifact.dataset.queries) == len(get_art.dataset.queries)
                assert len(artifact.model.name) == len(get_art.model.name)

                post_q = artifact.dataset.queries[0]
                get_q = get_art.dataset.queries[0]

                assert post_q.datasource_id == get_q.datasource_id
                assert len(post_q.features) == len(get_q.features)

                post_d = artifact.dict()
                get_d = get_art.dict()

                assert post_d == get_d

                # test model submit
                model_path = os.path.join('.', 'model.bin')
                model = {'model': 'example_model'}

                with open(model_path, 'wb') as f:
                    pickle.dump(model, f)

                res = client.post(
                    f'/worker/model/{artifact.artifact_id}',
                    headers=headers(token),
                    files={'file': open(model_path, 'rb')}
                )

                assert res.status_code == 200

                models: list[ModelDB] = session.query(ModelDB).all()

                assert len(models) == 1

                model_id = models[0].model_id

                # test model get
                res = client.get(
                    f'/worker/model/{model_id}',
                    headers=headers(token),
                )

                assert res.status_code == 200

                model_get = pickle.loads(res.content)

                assert isinstance(model_get, type(model))
                assert 'model' in model_get
                assert model == model_get

                assert os.path.exists(models[0].path)

                # cleanup
                os.remove(art_db.path)
                os.remove(models[0].path)
                os.remove(model_path)
