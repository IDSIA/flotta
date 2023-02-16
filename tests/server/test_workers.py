from ferdelance.database.data import TYPE_WORKER
from ferdelance.database.tables import (
    Artifact as ArtifactDB,
    DataSource,
    Token,
    Component,
    Model as ModelDB,
)
from ferdelance.database.services import DataSourceService
from ferdelance.server.api import api
from ferdelance.schemas.artifacts import (
    Artifact,
    ArtifactStatus,
)
from ferdelance.schemas.queries import (
    Query,
    QueryStage,
    QueryFeature,
)
from ferdelance.schemas.models import Model
from ferdelance.schemas.metadata import Metadata
from ferdelance.shared.exchange import Exchange
from ferdelance.shared.status import JobStatus

from tests.utils import (
    create_client,
    get_metadata,
    send_metadata,
)

from fastapi.testclient import TestClient
from requests import Response
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

import logging
import os
import pickle
import pytest
import uuid

LOGGER = logging.getLogger(__name__)


async def setup_worker(session: AsyncSession, exchange: Exchange):
    res = await session.execute(
        select(Token.token)
        .select_from(Token)
        .join(Component, Component.component_id == Token.component_id)
        .where(Component.type_name == TYPE_WORKER)
        .limit(1)
    )
    worker_token: str | None = res.scalar_one_or_none()

    assert worker_token is not None
    assert isinstance(worker_token, str)

    exchange.set_token(worker_token)


@pytest.mark.asyncio
async def test_worker_endpoints(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as server:
        create_client(server, exchange)

        metadata: Metadata = get_metadata()
        upload_response: Response = send_metadata(server, exchange, metadata)

        assert upload_response.status_code == 200

        await setup_worker(session, exchange)

        # test artifact not found
        res = server.get(
            f"/worker/artifact/{uuid.uuid4()}",
            headers=exchange.headers(),
        )

        assert res.status_code == 404

        # prepare new artifact
        res = await session.scalars(select(DataSource).limit(1))
        ds: DataSource = res.one()

        dss = DataSourceService(session)
        ds_data = await dss.load(ds.datasource_id)

        artifact = Artifact(
            artifact_id=None,
            transform=Query(
                stages=[
                    QueryStage(
                        features=[
                            QueryFeature(
                                name=f.name,
                                dtype=f.dtype,
                            )
                            for f in ds_data.features
                        ]
                    )
                ]
            ),
            model=Model(name="model", strategy=""),
            load=None,
        )

        # test artifact submit
        res = server.post("/worker/artifact", headers=exchange.headers(), json=artifact.dict())

        assert res.status_code == 200

        status: ArtifactStatus = ArtifactStatus(**res.json())

        LOGGER.info(f"artifact_id: {status.artifact_id}")

        artifact.artifact_id = status.artifact_id
        assert artifact.artifact_id is not None

        assert status.status is not None
        assert JobStatus[status.status] == JobStatus.SCHEDULED

        res = await session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact.artifact_id).limit(1))
        art_db: ArtifactDB | None = res.one()

        assert art_db is not None
        assert os.path.exists(art_db.path)

        # test artifact get
        res = server.get(
            f"/worker/artifact/{status.artifact_id}",
            headers=exchange.headers(),
        )

        assert res.status_code == 200

        get_art: Artifact = Artifact(**res.json())

        assert artifact.artifact_id == get_art.artifact_id

        assert len(artifact.transform.stages) == len(get_art.transform.stages)
        assert len(artifact.model.name) == len(get_art.model.name)

        post_d = artifact.dict()
        get_d = get_art.dict()

        assert post_d == get_d

        # test model submit
        model_path = os.path.join(".", "model.bin")
        model = {"model": "example_model"}

        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        res = server.post(
            f"/worker/model/{artifact.artifact_id}",
            headers=exchange.headers(),
            files={"file": open(model_path, "rb")},
        )

        assert res.status_code == 200

        res = await session.scalars(select(ModelDB))
        models: list[ModelDB] = list(res.all())

        assert len(models) == 1

        model_id = models[0].model_id

        # test model get
        res = server.get(
            f"/worker/model/{model_id}",
            headers=exchange.headers(),
        )

        assert res.status_code == 200

        model_get = pickle.loads(res.content)

        assert isinstance(model_get, type(model))
        assert "model" in model_get
        assert model == model_get

        assert os.path.exists(models[0].path)

        # cleanup
        os.remove(art_db.path)
        os.remove(models[0].path)
        os.remove(model_path)


@pytest.mark.asyncio
async def test_worker_access(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as server:
        await setup_worker(session, exchange)

        res = server.get(
            "/client/update",
            headers=exchange.headers(),
        )

        assert res.status_code == 403

        res = server.get(
            "/worker/artifact/none",
            headers=exchange.headers(),
        )

        assert res.status_code == 404  # there is no artifact, and 404 is correct

        res = server.get(
            "/workbench/client/list",
            headers=exchange.headers(),
        )

        assert res.status_code == 403
