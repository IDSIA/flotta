from ferdelance.database.tables import (
    Artifact as ArtifactDB,
    Result as ResultDB,
)
from ferdelance.database.repositories import ProjectRepository
from ferdelance.server.api import api
from ferdelance.schemas.artifacts import Artifact, ArtifactStatus
from ferdelance.schemas.models import Model
from ferdelance.shared.exchange import Exchange
from ferdelance.shared.status import JobStatus

from tests.utils import setup_worker, connect, TEST_PROJECT_TOKEN

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import logging
import os
import pickle
import pytest
import uuid

LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_worker_artifact_not_found(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as server:
        await setup_worker(session, exchange)

        res = server.get(
            f"/worker/artifact/{uuid.uuid4()}",
            headers=exchange.headers(),
        )

        assert res.status_code == 404


@pytest.mark.asyncio
async def test_worker_endpoints(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as server:
        await connect(server, session)
        await setup_worker(session, exchange)

        # prepare new artifact
        pr: ProjectRepository = ProjectRepository(session)
        project = await pr.get_by_token(TEST_PROJECT_TOKEN)

        artifact = Artifact(
            artifact_id=None,
            project_id=project.project_id,
            transform=project.data.extract(),
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

        assert artifact.model is not None
        assert get_art.model is not None
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
            f"/worker/result/{artifact.artifact_id}",
            headers=exchange.headers(),
            files={"file": open(model_path, "rb")},
        )

        assert res.status_code == 200

        res = await session.scalars(select(ResultDB))
        results: list[ResultDB] = list(res.all())

        assert len(results) == 1

        result_id = results[0].result_id

        # test model get
        res = server.get(
            f"/worker/result/{result_id}",
            headers=exchange.headers(),
        )

        assert res.status_code == 200

        model_get = pickle.loads(res.content)

        assert isinstance(model_get, type(model))
        assert "model" in model_get
        assert model == model_get

        assert os.path.exists(results[0].path)

        # cleanup
        os.remove(art_db.path)
        os.remove(results[0].path)
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
            "/workbench/clients",
            headers=exchange.headers(),
        )

        assert res.status_code == 403
