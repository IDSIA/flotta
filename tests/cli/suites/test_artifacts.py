from ferdelance.cli.fdl_suites.artifacts.functions import describe_artifact, list_artifacts
from ferdelance.database.tables import Artifact as ArtifactDB
from ferdelance.schemas.database import ServerArtifact

from sqlalchemy.ext.asyncio import AsyncSession

import pytest


@pytest.mark.asyncio
async def test_artifacts_ls(session: AsyncSession):
    artifact_id_1: str = "artifact1"
    artifact_id_2: str = "artifact2"

    session.add(
        ArtifactDB(
            artifact_id=artifact_id_1,
            path=".",
            status="",
        )
    )
    session.add(
        ArtifactDB(
            artifact_id=artifact_id_2,
            path=".",
            status="",
        )
    )

    await session.commit()

    res = await list_artifacts()

    assert len(res) == 2


@pytest.mark.asyncio
async def test_artifacts_description(session: AsyncSession):
    artifact_id_1: str = "artifact1"
    artifact_id_2: str = "artifact2"

    session.add(
        ArtifactDB(
            artifact_id=artifact_id_1,
            path=".",
            status="",
        )
    )
    session.add(
        ArtifactDB(
            artifact_id=artifact_id_2,
            path=".",
            status="",
        )
    )

    await session.commit()

    res: ServerArtifact | None = await describe_artifact(artifact_id=artifact_id_1)

    assert res is not None
    assert res.id == "artifact1"
    assert res.path == "."
    assert res.status == ""

    with pytest.raises(ValueError) as e:
        res = await describe_artifact(artifact_id=None)
        assert "Provide an Artifact ID" in str(e)

    res = await describe_artifact(artifact_id="do_not_exist")

    assert res is None
