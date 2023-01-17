import pandas as pd
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from ferdelance.cli.artifacts.functions import (
    get_artifact_description,
    get_artifacts_list,
)
from ferdelance.database.tables import Artifact


@pytest.mark.asyncio
async def test_artifacts_ls(async_session: AsyncSession):
    artifact_id_1: str = "artifact1"
    artifact_id_2: str = "artifact2"

    async_session.add(
        Artifact(
            artifact_id=artifact_id_1,
            path=".",
            status="",
        )
    )
    async_session.add(
        Artifact(
            artifact_id=artifact_id_2,
            path=".",
            status="",
        )
    )

    await async_session.commit()

    res: pd.DataFrame = await get_artifacts_list()

    assert len(res) == 2


@pytest.mark.asyncio
async def test_artifacts_description(async_session: AsyncSession):
    artifact_id_1: str = "artifact1"
    artifact_id_2: str = "artifact2"

    async_session.add(
        Artifact(
            artifact_id=artifact_id_1,
            path=".",
            status="",
        )
    )
    async_session.add(
        Artifact(
            artifact_id=artifact_id_2,
            path=".",
            status="",
        )
    )

    await async_session.commit()

    res: Artifact = await get_artifact_description(artifact_id=artifact_id_1)

    assert res.artifact_id == "artifact1"
    assert res.path == "."
    assert res.status == ""

    with pytest.raises(ValueError) as e:
        res = await get_artifact_description()
    assert "artifact_id is None, must have a value" in str(e)

    res = await get_artifact_description(artifact_id="do_not_exist")

    assert res is None
