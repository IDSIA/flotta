from ferdelance.cli.fdl_suites.models.functions import describe_model, list_models
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.database.schemas import Model as ModelView
from ferdelance.database.tables import Artifact, Component, Model

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import NoResultFound

import pytest


@pytest.mark.asyncio
async def test_models_list(async_session: AsyncSession):

    async_session.add(
        Artifact(
            artifact_id="aid1",
            path=".",
            status="",
        )
    )

    async_session.add(
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

    async_session.add(Model(model_id="mid1", path=".", artifact_id="aid1", component_id="cid1"))

    res: list[ModelView] = await list_models()

    assert len(res) == 0

    await async_session.commit()

    res: list[ModelView] = await list_models()

    assert len(res) == 1


@pytest.mark.asyncio
async def test_describe_client(async_session: AsyncSession):
    async_session.add(
        Artifact(
            artifact_id="aid1",
            path=".",
            status="",
        )
    )

    async_session.add(
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

    async_session.add(
        Model(model_id="mid1", path=".", artifact_id="aid1", component_id="cid1", aggregated=True, creation_time=None)
    )

    with pytest.raises(NoResultFound):
        res: ModelView = await describe_model(model_id="mid1")

    await async_session.commit()

    res: ModelView = await describe_model(model_id="mid1")

    assert res.model_id == "mid1"
    assert res.path == "."

    with pytest.raises(NoResultFound) as e:
        res = await describe_model(model_id="do not exist")
        assert "No row was found when one was required" in str(e)
