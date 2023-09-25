from ferdelance.cli.fdl_suites.models.functions import describe_model, list_models
from ferdelance.const import TYPE_CLIENT
from ferdelance.schemas.database import Result
from ferdelance.database.tables import Artifact, Component, Result as ResultDB

from sqlalchemy.ext.asyncio import AsyncSession

import pytest


@pytest.mark.asyncio
async def test_models_list(session: AsyncSession):
    session.add(
        Artifact(
            id="aid1",
            path=".",
            status="",
        )
    )

    session.add(
        Component(
            id="cid1",
            name="client1",
            version="test",
            public_key="1",
            ip_address="1",
            url="",
            type_name=TYPE_CLIENT,
        )
    )

    session.add(
        ResultDB(
            id="mid1",
            job_id="job-1",
            path=".",
            artifact_id="aid1",
            component_id="cid1",
            is_model=True,
        )
    )

    res: list[Result] = await list_models()

    assert len(res) == 0

    await session.commit()

    res: list[Result] = await list_models()

    assert len(res) == 1


@pytest.mark.asyncio
async def test_describe_client(session: AsyncSession):
    session.add(
        Artifact(
            id="aid1",
            path=".",
            status="",
        )
    )

    session.add(
        Component(
            id="cid1",
            name="client1",
            version="test",
            public_key="1",
            ip_address="1",
            url="",
            type_name=TYPE_CLIENT,
        )
    )

    session.add(
        ResultDB(
            id="mid1",
            job_id="job-1",
            path=".",
            artifact_id="aid1",
            component_id="cid1",
            is_model=True,
            is_aggregation=True,
            creation_time=None,
        )
    )

    await session.commit()

    res: Result | None = await describe_model(task_id="mid1")

    assert res is not None
    assert res.id == "mid1"
    assert res.path == "."

    res = await describe_model(task_id="do not exist")
    assert res is None
