from ferdelance.cli.fdl_suites.models.functions import describe_model, list_models
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.schemas.database import ServerTask
from ferdelance.database.tables import Artifact, Component, Result

from sqlalchemy.ext.asyncio import AsyncSession

import pytest


@pytest.mark.asyncio
async def test_models_list(session: AsyncSession):

    session.add(
        Artifact(
            artifact_id="aid1",
            path=".",
            status="",
        )
    )

    session.add(
        Component(
            component_id="cid1",
            version="test",
            public_key="1",
            machine_system="1",
            machine_mac_address="1",
            machine_node="1",
            ip_address="1",
            type_name=TYPE_CLIENT,
        )
    )

    session.add(
        Result(
            model_id="mid1",
            path=".",
            artifact_id="aid1",
            component_id="cid1",
            is_model=True,
        )
    )

    res: list[ServerTask] = await list_models()

    assert len(res) == 0

    await session.commit()

    res: list[ServerTask] = await list_models()

    assert len(res) == 1


@pytest.mark.asyncio
async def test_describe_client(session: AsyncSession):
    session.add(
        Artifact(
            artifact_id="aid1",
            path=".",
            status="",
        )
    )

    session.add(
        Component(
            component_id="cid1",
            version="test",
            public_key="1",
            machine_system="1",
            machine_mac_address="1",
            machine_node="1",
            ip_address="1",
            type_name="CLIENT",
        )
    )

    session.add(
        Result(
            model_id="mid1",
            path=".",
            artifact_id="aid1",
            component_id="cid1",
            aggregated=True,
            creation_time=None,
            is_model=True,
        )
    )

    await session.commit()

    res: ServerTask | None = await describe_model(task_id="mid1")

    assert res is not None
    assert res.model_id == "mid1"
    assert res.path == "."

    res = await describe_model(task_id="do not exist")
    assert res is None
