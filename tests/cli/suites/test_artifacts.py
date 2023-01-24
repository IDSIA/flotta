from ferdelance.cli.fdl_suites.artifacts.functions import describe_artifact, list_artifacts
from ferdelance.database.tables import Artifact as ArtifactDB
from ferdelance.database.schemas import Artifact

from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

import pytest


@pytest.mark.asyncio
async def test_artifacts_ls(async_session: AsyncSession):
    artifact_id_1: str = "artifact1"
    artifact_id_2: str = "artifact2"

    async_session.add(
        ArtifactDB(
            artifact_id=artifact_id_1,
            path=".",
            status="",
        )
    )
    async_session.add(
        ArtifactDB(
            artifact_id=artifact_id_2,
            path=".",
            status="",
        )
    )

    await async_session.commit()

    res = await list_artifacts()

    assert len(res) == 2


@pytest.mark.asyncio
async def test_artifacts_description(async_session: AsyncSession):
    artifact_id_1: str = "artifact1"
    artifact_id_2: str = "artifact2"

    async_session.add(
        ArtifactDB(
            artifact_id=artifact_id_1,
            path=".",
            status="",
        )
    )
    async_session.add(
        ArtifactDB(
            artifact_id=artifact_id_2,
            path=".",
            status="",
        )
    )

    await async_session.commit()

    res: Artifact = await describe_artifact(artifact_id=artifact_id_1)

    assert res.artifact_id == "artifact1"
    assert res.path == "."
    assert res.status == ""

    with pytest.raises(ValueError) as e:
        res = await describe_artifact(artifact_id=None)
        assert "Provide an Artifact ID" in str(e)

    with pytest.raises(NoResultFound):
        await describe_artifact(artifact_id="do_not_exist")
