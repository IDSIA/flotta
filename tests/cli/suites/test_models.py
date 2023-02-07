from ferdelance.cli.fdl_suites.models.functions import describe_model, list_models
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.database.schemas import Model as Model
from ferdelance.database.tables import Artifact, Component, Model

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

    session.add(Model(model_id="mid1", path=".", artifact_id="aid1", component_id="cid1"))

    res: list[Model] = await list_models()

    assert len(res) == 0

    await session.commit()

    res: list[Model] = await list_models()

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
        Model(model_id="mid1", path=".", artifact_id="aid1", component_id="cid1", aggregated=True, creation_time=None)
    )

    await session.commit()

    res: Model = await describe_model(model_id="mid1")

    assert res.model_id == "mid1"
    assert res.path == "."

    res = await describe_model(model_id="do not exist")
    assert res is None
